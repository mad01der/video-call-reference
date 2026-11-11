package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/oggwriter"
)

var (
	micOn     atomic.Bool
	speakerOn atomic.Bool
)

func init() {
	micOn.Store(true)
	speakerOn.Store(true)
}

func startMic(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	go func() {
		for ctx.Err() == nil {
			err := sendMic(ctx, track)
			if ctx.Err() != nil {
				return
			}
			logf("麦克风停止：%v，1 秒后重新打开", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
}

func sendMic(ctx context.Context, track *webrtc.TrackLocalStaticSample) error {
	input, name, err := micInput()
	if err != nil {
		return err
	}
	logf("使用麦克风：%s", name)
	args := []string{"-hide_banner", "-loglevel", "warning"}
	args = append(args, input...)
	args = append(args,
		"-ac", "2", "-ar", "48000",
		"-c:a", "libopus", "-b:a", "32k", "-vbr", "off",
		"-application", "voip", "-frame_duration", "20",
		"-f", "ogg", "pipe:1",
	)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = decodeLog()
	if err := cmd.Start(); err != nil {
		return err
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	for {
		packets, err := readOggPackets(stdout)
		for _, pkt := range packets {
			if len(pkt) >= 8 && (string(pkt[:8]) == "OpusHead" || string(pkt[:8]) == "OpusTags") {
				continue
			}
			if !micOn.Load() {
				continue
			}
			if werr := track.WriteSample(media.Sample{Data: pkt, Duration: 20 * time.Millisecond}); werr != nil {
				logf("发送一帧音频失败：%v", werr)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return <-waitErr
			}
			return err
		}
	}
}

func micInput() ([]string, string, error) {
	switch runtime.GOOS {
	case "darwin":
		idx, name, err := darwinMic()
		if err != nil {
			return nil, "", err
		}
		return []string{"-f", "avfoundation", "-i", fmt.Sprintf(":%d", idx)}, name, nil
	case "linux":
		return []string{"-f", "pulse", "-i", "default"}, "默认麦克风", nil
	default:
		return nil, "", errors.New("当前系统没有自动打开麦克风的方法")
	}
}

func darwinMic() (int, string, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", "")
	out, _ := cmd.CombinedOutput()
	inAudio := false
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "AVFoundation audio devices") {
			inAudio = true
			continue
		}
		if !inAudio {
			continue
		}
		bracket := strings.Index(line, "] [")
		if bracket < 0 {
			continue
		}
		rest := line[bracket+3:]
		var idx int
		if _, err := fmt.Sscanf(rest, "%d]", &idx); err != nil {
			continue
		}
		name := rest
		if mark := strings.Index(rest, "] "); mark >= 0 {
			name = strings.TrimSpace(rest[mark+2:])
		}
		if name == "" {
			continue
		}
		return idx, name, nil
	}
	return 0, "", errors.New("没有找到麦克风")
}

// readOggPackets 读完一页 Ogg，按分段表拆成 Opus 包。
func readOggPackets(r io.Reader) ([][]byte, error) {
	var hdr [27]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if string(hdr[0:4]) != "OggS" {
		return nil, errors.New("麦克风输出不是 Ogg")
	}
	nseg := int(hdr[26])
	table := make([]byte, nseg)
	if _, err := io.ReadFull(r, table); err != nil {
		return nil, err
	}
	total := 0
	for _, s := range table {
		total += int(s)
	}
	payload := make([]byte, total)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	var pkts [][]byte
	var cur []byte
	off := 0
	for _, lace := range table {
		cur = append(cur, payload[off:off+int(lace)]...)
		off += int(lace)
		if lace < 255 {
			pkts = append(pkts, cur)
			cur = nil
		}
	}
	return pkts, nil
}

func handleAudio(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req struct {
			Mic     *bool `json:"mic"`
			Speaker *bool `json:"speaker"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 512)).Decode(&req); err != nil {
			http.Error(w, "音频开关内容不正确", http.StatusBadRequest)
			return
		}
		if req.Mic != nil {
			micOn.Store(*req.Mic)
			if *req.Mic {
				logf("麦克风已打开")
			} else {
				logf("麦克风已关闭")
			}
		}
		if req.Speaker != nil {
			speakerOn.Store(*req.Speaker)
			if *req.Speaker {
				logf("扬声器已打开")
			} else {
				logf("扬声器已关闭")
			}
		}
	}
	writeJSON(w, map[string]bool{
		"mic":     micOn.Load(),
		"speaker": speakerOn.Load(),
	})
}

func playRemoteAudio(ctx context.Context, track *webrtc.TrackRemote) {
	cmd := exec.CommandContext(ctx, "ffplay",
		"-nodisp", "-loglevel", "error",
		"-fflags", "nobuffer", "-flags", "low_delay",
		"-probesize", "32", "-analyzeduration", "0",
		"-f", "ogg", "-i", "pipe:0",
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		logf("播放声音失败：%v", err)
		return
	}
	cmd.Stderr = decodeLog()
	if err := cmd.Start(); err != nil {
		logf("播放声音失败：%v", err)
		return
	}
	go func() { _ = cmd.Wait() }()

	channels := uint16(track.Codec().Channels)
	if channels == 0 {
		channels = 2
	}
	writer, err := oggwriter.NewWith(stdin, 48000, channels)
	if err != nil {
		logf("播放声音失败：%v", err)
		return
	}
	defer writer.Close()
	logf("开始播放对方声音")
	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if ctx.Err() == nil {
				logf("声音接收结束：%v", err)
			}
			return
		}
		if !speakerOn.Load() {
			continue
		}
		if err := writer.WriteRTP(pkt); err != nil {
			logf("写入声音失败：%v", err)
			return
		}
	}
}
