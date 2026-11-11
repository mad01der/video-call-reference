package main

import (
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/webrtc/v4"
)

const (
	initialVideoBitrate = 500_000
	minVideoBitrate     = 180_000
	maxVideoBitrate     = 1_200_000
	audioBitrate        = 32_000
)

// videoBitrate 是拥塞控制交给编码器的目标，单位比特每秒。
var videoBitrate atomic.Int64

func init() {
	videoBitrate.Store(initialVideoBitrate)
}

func currentVideoBitrate() int {
	b := int(videoBitrate.Load())
	if b < minVideoBitrate {
		return minVideoBitrate
	}
	if b > maxVideoBitrate {
		return maxVideoBitrate
	}
	return b
}

// installCongestion 在默认拦截器之前装上漏桶和发送端拥塞控制。
// 漏桶负责把一帧里的 RTP 匀开，拥塞控制根据 TWCC 反馈改目标码率。
func installCongestion(mediaEngine *webrtc.MediaEngine, registry *interceptor.Registry) error {
	factory, err := cc.NewInterceptor(func() (cc.BandwidthEstimator, error) {
		pacer := gcc.NewLeakyBucketPacer(initialVideoBitrate + audioBitrate)
		return gcc.NewSendSideBWE(
			gcc.SendSideBWEInitialBitrate(initialVideoBitrate+audioBitrate),
			gcc.SendSideBWEMinBitrate(minVideoBitrate+audioBitrate),
			gcc.SendSideBWEMaxBitrate(maxVideoBitrate+audioBitrate),
			gcc.SendSideBWEPacer(pacer),
		)
	})
	if err != nil {
		return err
	}
	var last atomic.Int64
	last.Store(initialVideoBitrate)
	factory.OnNewPeerConnection(func(_ string, est cc.BandwidthEstimator) {
		est.OnTargetBitrateChange(func(total int) {
			video := total - audioBitrate
			if video < minVideoBitrate {
				video = minVideoBitrate
			}
			if video > maxVideoBitrate {
				video = maxVideoBitrate
			}
			prev := int(last.Swap(int64(video)))
			videoBitrate.Store(int64(video))
			if prev > 0 && absInt(prev-video)*5 > prev {
				logf("拥塞控制把视频码率调到 %d kbps", video/1000)
			}
		})
	})
	registry.Add(factory)
	return webrtc.ConfigureTWCCHeaderExtensionSender(mediaEngine, registry)
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// drainRTCP 把发送端的 RTCP 读走。不读的话，TWCC 反馈到不了拥塞控制。
func drainRTCP(sender *webrtc.RTPSender) {
	if sender == nil {
		return
	}
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()
}
