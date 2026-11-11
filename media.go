package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image/jpeg"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// 电脑发出去的画面大小。改这里之后重新运行，状态页的 send 会跟着变。
const (
	frameWidth  = 640
	frameHeight = 480
	frameRate   = 24
)

var errRestart = errors.New("码率变了，重新编码")

// 摄像头一次只能被一个 ffmpeg 打开，所以通话选定编码后再启动对应的那一路。
var (
	encMu     sync.Mutex
	encCancel context.CancelFunc
	encKind   string
)

func useEncoder(ctx context.Context, kind string, track *webrtc.TrackLocalStaticSample, source string) {
	encMu.Lock()
	defer encMu.Unlock()
	if encCancel != nil && encKind == kind {
		return
	}
	if encCancel != nil {
		encCancel()
	}
	cctx, cancel := context.WithCancel(ctx)
	encCancel = cancel
	encKind = kind
	if kind == "vp8" {
		go startVP8Encoder(cctx, track, source)
		return
	}
	go startEncoder(cctx, track, source)
}

func startEncoder(ctx context.Context, track *webrtc.TrackLocalStaticSample, source string) {
	for ctx.Err() == nil {
		err := encodeOnce(ctx, track, source)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errRestart) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
			continue
		}
		logf("编码器停止：%v，1 秒后重新启动", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func encodeOnce(ctx context.Context, track *webrtc.TrackLocalStaticSample, source string) error {
	input, used, err := encoderInput(source)
	if err != nil {
		return err
	}
	setSource(used)
	args := []string{"-hide_banner", "-loglevel", "warning"}
	args = append(args, input...)
	args = append(args,
		"-vf", fmt.Sprintf("scale=%d:%d,format=yuv420p", frameWidth, frameHeight),
		"-r", fmt.Sprintf("%d", frameRate),
		"-c:v", "libx264",
		"-profile:v", "baseline",
		"-level", "3.1",
		"-pix_fmt", "yuv420p",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-b:v", fmt.Sprintf("%dk", currentVideoBitrate()/1000),
		"-maxrate", fmt.Sprintf("%dk", currentVideoBitrate()/1000),
		"-bufsize", fmt.Sprintf("%dk", currentVideoBitrate()/1000/10),
		"-g", "48",
		"-keyint_min", "48",
		"-bf", "0",
		"-x264-params", "repeat-headers=1:sliced-threads=0",
		"-an",
		"-f", "h264",
		"pipe:1",
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
	logf("开始用 H.264 编码，来源是%s，分辨率 %d×%d", used, frameWidth, frameHeight)

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	stream := &h264Stream{}
	buf := make([]byte, 32*1024)
	started := currentVideoBitrate()
	startedAt := time.Now()
	for {
		n, readErr := stdout.Read(buf)
		for _, au := range stream.Push(buf[:n]) {
			addOutFrame()
			if werr := track.WriteSample(media.Sample{Data: au, Duration: time.Second / frameRate}); werr != nil {
				logf("发送一帧失败：%v", werr)
			}
		}
		if time.Since(startedAt) >= 12*time.Second && bitrateShifted(started) {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return errRestart
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return <-waitErr
}

func startVP8Encoder(ctx context.Context, track *webrtc.TrackLocalStaticSample, source string) {
	for ctx.Err() == nil {
		err := encodeVP8Once(ctx, track, source)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errRestart) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
			continue
		}
		logf("VP8 编码器停止：%v，1 秒后重新启动", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func encodeVP8Once(ctx context.Context, track *webrtc.TrackLocalStaticSample, source string) error {
	input, used, err := encoderInput(source)
	if err != nil {
		return err
	}
	setSource(used)
	args := []string{"-hide_banner", "-loglevel", "warning"}
	args = append(args, input...)
	args = append(args,
		"-vf", fmt.Sprintf("scale=%d:%d,format=yuv420p", frameWidth, frameHeight),
		"-r", fmt.Sprintf("%d", frameRate),
		"-c:v", "libvpx",
		"-deadline", "realtime",
		"-cpu-used", "5",
		"-lag-in-frames", "0",
		"-error-resilient", "1",
		"-auto-alt-ref", "0",
		"-b:v", fmt.Sprintf("%dk", currentVideoBitrate()/1000),
		"-maxrate", fmt.Sprintf("%dk", currentVideoBitrate()/1000),
		"-bufsize", fmt.Sprintf("%dk", currentVideoBitrate()/10000),
		"-g", "48",
		"-keyint_min", "48",
		"-an",
		"-f", "ivf",
		"pipe:1",
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
	logf("开始用 VP8 编码，来源是%s，分辨率 %d×%d", used, frameWidth, frameHeight)
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	reader := &ivfReader{}
	buf := make([]byte, 32*1024)
	started := currentVideoBitrate()
	startedAt := time.Now()
	for {
		n, readErr := stdout.Read(buf)
		for _, frame := range reader.Push(buf[:n]) {
			addOutFrame()
			if werr := track.WriteSample(media.Sample{Data: frame, Duration: time.Second / frameRate}); werr != nil {
				logf("发送 VP8 一帧失败：%v", werr)
			}
		}
		if time.Since(startedAt) >= 12*time.Second && bitrateShifted(started) {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return errRestart
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return <-waitErr
}

// ivfReader 从 ffmpeg 的 IVF 输出里取出一帧帧 VP8。
type ivfReader struct {
	buf    []byte
	header bool
}

func (r *ivfReader) Push(p []byte) [][]byte {
	if len(p) == 0 {
		return nil
	}
	r.buf = append(r.buf, p...)
	var frames [][]byte
	for {
		if !r.header {
			if len(r.buf) < 32 {
				return frames
			}
			r.buf = append([]byte(nil), r.buf[32:]...)
			r.header = true
		}
		if len(r.buf) < 12 {
			return frames
		}
		size := int(binary.LittleEndian.Uint32(r.buf[:4]))
		if size < 0 || len(r.buf) < 12+size {
			return frames
		}
		frames = append(frames, append([]byte(nil), r.buf[12:12+size]...))
		r.buf = append([]byte(nil), r.buf[12+size:]...)
	}
}

// encoderInput 返回 ffmpeg 的输入参数，以及给学生看的来源名称。
func bitrateShifted(running int) bool {
	if running <= 0 {
		return false
	}
	// 估计值小幅抖动时不重启编码器，避免画面被反复打断。
	return absInt(currentVideoBitrate()-running)*2 > running
}

func encoderInput(source string) ([]string, string, error) {
	if source != "camera" {
		return nil, "", fmt.Errorf("不认识的画面来源 %q，请使用 camera", source)
	}
	in, err := cameraInput()
	if err != nil {
		return nil, "", err
	}
	return in, "摄像头", nil
}

func cameraInput() ([]string, error) {
	switch runtime.GOOS {
	case "darwin":
		idx, name, err := darwinCamera()
		if err != nil {
			return nil, err
		}
		logf("使用摄像头：%s", name)
		return []string{"-use_wallclock_as_timestamps", "1", "-f", "avfoundation", "-framerate", "30", "-pixel_format", "uyvy422", "-i", fmt.Sprintf("%d:none", idx)}, nil
	case "linux":
		if _, err := os.Stat("/dev/video0"); err != nil {
			return nil, errors.New("没有找到 /dev/video0")
		}
		return []string{
			"-f", "v4l2",
			"-framerate", "30",
			"-video_size", fmt.Sprintf("%dx%d", frameWidth, frameHeight),
			"-i", "/dev/video0",
		}, nil
	default:
		return nil, errors.New("当前系统没有自动识别摄像头的方法")
	}
}

func darwinCamera() (int, string, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", "")
	out, _ := cmd.CombinedOutput()
	lines := strings.Split(string(out), "\n")
	inVideo := false
	for _, line := range lines {
		if strings.Contains(line, "AVFoundation video devices") {
			inVideo = true
			continue
		}
		if strings.Contains(line, "AVFoundation audio devices") {
			break
		}
		if !inVideo {
			continue
		}
		bracket := strings.Index(line, "] [")
		if bracket < 0 {
			continue
		}
		rest := line[bracket+3:]
		var idx int
		var name string
		if _, err := fmt.Sscanf(rest, "%d] %s", &idx, &name); err != nil {
			continue
		}
		// Sscanf 只读到空格。把设备名整段取出来。
		if mark := strings.Index(rest, "] "); mark >= 0 {
			name = strings.TrimSpace(rest[mark+2:])
		}
		if name == "" || strings.Contains(name, "Capture screen") {
			continue
		}
		return idx, name, nil
	}
	return 0, "", errors.New("没有找到摄像头")
}

// h264Stream 把 ffmpeg 吐出的 Annex-B 字节流切成一帧一帧。
// 一帧里可以带着 SPS、PPS 和图像数据，交给 Pion 发送。
type h264Stream struct {
	buf     []byte
	pending [][]byte
}

func (h *h264Stream) Push(p []byte) [][]byte {
	if len(p) == 0 {
		return nil
	}
	h.buf = append(h.buf, p...)
	var aus [][]byte
	for {
		nal, ok := h.popNAL()
		if !ok {
			break
		}
		if len(nal) == 0 {
			continue
		}
		kind := nal[0] & 0x1f
		// 6 是编码器信息，9 是分隔符。手机硬解码遇到这些会解不出画面。
		if kind == 6 || kind == 9 || kind == 12 {
			continue
		}
		h.pending = append(h.pending, nal)
		if kind >= 1 && kind <= 5 {
			aus = append(aus, joinAnnexB(h.pending))
			h.pending = nil
		}
	}
	return aus
}

func (h *h264Stream) popNAL() ([]byte, bool) {
	for {
		start, length := indexStartCode(h.buf, 0)
		if start < 0 {
			if len(h.buf) > 3 {
				h.buf = append([]byte(nil), h.buf[len(h.buf)-3:]...)
			}
			return nil, false
		}
		if start > 0 {
			h.buf = append([]byte(nil), h.buf[start:]...)
			continue
		}
		next, _ := indexStartCode(h.buf, length)
		if next < 0 {
			return nil, false
		}
		nal := append([]byte(nil), h.buf[length:next]...)
		h.buf = append([]byte(nil), h.buf[next:]...)
		return nal, true
	}
}

func indexStartCode(b []byte, from int) (pos, length int) {
	for i := from; i+3 <= len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			if i > 0 && b[i-1] == 0 {
				return i - 1, 4
			}
			return i, 3
		}
	}
	return -1, 0
}

func h264Key(frame []byte) bool {
	for i := 0; i+4 < len(frame); i++ {
		if frame[i] != 0 || frame[i+1] != 0 {
			continue
		}
		off := -1
		if frame[i+2] == 1 {
			off = i + 3
		} else if frame[i+2] == 0 && frame[i+3] == 1 {
			off = i + 4
		}
		if off >= 0 && off < len(frame) && frame[off]&0x1f == 5 {
			return true
		}
	}
	return false
}

func joinAnnexB(nals [][]byte) []byte {
	var out []byte
	for _, nal := range nals {
		out = append(out, 0, 0, 0, 1)
		out = append(out, nal...)
	}
	return out
}

// decodeSink 把收到的压缩帧按顺序送给播放窗口和预览。不能丢掉中间帧，否则会花屏。
type decodeSink struct {
	ctx         context.Context
	mu          sync.Mutex
	player      io.WriteCloser
	preview     io.WriteCloser
	previewCh   chan []byte
	previewDead atomic.Bool
	close       func()
}

func decodeLog() io.Writer {
	f, err := os.OpenFile(filepath.Join(os.TempDir(), "course-call-decode.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return io.Discard
	}
	return f
}

func startDecoder(ctx context.Context, preview string, window bool, format string) (*decodeSink, error) {
	sink := &decodeSink{ctx: ctx, previewCh: make(chan []byte, 60)}
	var cancels []context.CancelFunc
	open := func(args ...string) (io.WriteCloser, error) {
		cctx, cancel := context.WithCancel(ctx)
		cmd := exec.CommandContext(cctx, args[0], args[1:]...)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			cancel()
			return nil, err
		}
		cmd.Stderr = decodeLog()
		if err := cmd.Start(); err != nil {
			cancel()
			return nil, err
		}
		go func() { _ = cmd.Wait() }()
		cancels = append(cancels, cancel)
		return stdin, nil
	}

	previewArgs := []string{"ffmpeg", "-hide_banner", "-loglevel", "error"}
	if format == "h264" {
		// 原始 H.264 没有时间戳。不指定帧率时，后面的帧会被当成同一时刻，预览停在第一张。
		previewArgs = append(previewArgs, "-framerate", fmt.Sprintf("%d", frameRate))
	} else {
		previewArgs = append(previewArgs,
			"-fflags", "nobuffer", "-flags", "low_delay",
			"-probesize", "32768", "-analyzeduration", "0",
		)
	}
	previewArgs = append(previewArgs, "-f", format, "-i", "pipe:0", "-an", "-strict", "unofficial", "-q:v", "5")
	if format != "h264" {
		previewArgs = append(previewArgs, "-fps_mode", "passthrough")
	}
	previewArgs = append(previewArgs, "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1")
	previewIn, previewOut, cancelPreview, err := startPipe(ctx, previewArgs...)
	if err != nil {
		return nil, err
	}
	cancels = append(cancels, cancelPreview)
	sink.preview = previewIn
	go writePreviewJPEGs(previewOut, preview)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-sink.previewCh:
				if !ok {
					return
				}
				if sink.previewDead.Load() {
					continue
				}
				if _, werr := previewIn.Write(frame); werr != nil {
					sink.previewDead.Store(true)
					logf("预览解码停止：%v", werr)
				}
			}
		}
	}()
	if window {
		playArgs := []string{"ffplay", "-hide_banner", "-loglevel", "error"}
		if format == "h264" {
			playArgs = append(playArgs, "-framerate", fmt.Sprintf("%d", frameRate))
		} else {
			playArgs = append(playArgs,
				"-fflags", "nobuffer", "-flags", "low_delay",
				"-probesize", "32768", "-analyzeduration", "0",
			)
		}
		playArgs = append(playArgs,
			"-f", format, "-i", "pipe:0",
			"-window_title", "phone", "-left", "40", "-top", "80",
			"-x", "640", "-y", "480",
		)
		player, err := open(playArgs...)
		if err != nil {
			logf("没有弹出窗口：%v", err)
		} else {
			sink.player = player
		}
	}
	sink.close = func() {
		for _, c := range cancels {
			c()
		}
	}
	return sink, nil
}

func (s *decodeSink) Write(p []byte) {
	if len(p) == 0 || s == nil {
		return
	}
	s.mu.Lock()
	player := s.player
	s.mu.Unlock()
	if player != nil {
		_, _ = player.Write(p)
	}
	if s.previewDead.Load() {
		return
	}
	select {
	case <-s.ctx.Done():
	case s.previewCh <- append([]byte(nil), p...):
	}
}

func startPipe(ctx context.Context, args ...string) (io.WriteCloser, io.ReadCloser, context.CancelFunc, error) {
	cctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(cctx, args[0], args[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	cmd.Stderr = decodeLog()
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, nil, nil, err
	}
	go func() { _ = cmd.Wait() }()
	return stdin, stdout, cancel, nil
}

var previewOnce sync.Once

func writePreviewJPEGs(r io.Reader, path string) {
	var buf []byte
	tmp := make([]byte, 32*1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		for {
			start := bytes.Index(buf, []byte{0xff, 0xd8})
			if start < 0 {
				if len(buf) > 1 {
					buf = buf[len(buf)-1:]
				}
				break
			}
			if start > 0 {
				buf = buf[start:]
			}
			end := bytes.Index(buf, []byte{0xff, 0xd9})
			if end < 2 {
				break
			}
			end += 2
			frame := append([]byte(nil), buf[:end]...)
			buf = buf[end:]
			savePreview(path, frame)
		}
		if len(buf) > 8<<20 {
			buf = nil
		}
		if err != nil {
			return
		}
	}
}

func savePreview(path string, frame []byte) {
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(frame))
	if err != nil {
		return
	}
	phoneVideo.setSize(cfg.Width, cfg.Height)
	part := path + ".part"
	if err := os.WriteFile(part, frame, 0o644); err != nil {
		return
	}
	if err := os.Rename(part, path); err != nil {
		return
	}
	previewOnce.Do(func() { logf("手机画面预览已更新") })
}

func (s *decodeSink) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.player != nil {
		_ = s.player.Close()
	}
	if s.preview != nil {
		_ = s.preview.Close()
	}
	s.mu.Unlock()
	if s.close != nil {
		s.close()
	}
}

// writeFrames 按到达顺序把每一帧交给播放和预览。中间丢掉参考帧会解出花屏。
func writeFrames(ctx context.Context, sink *decodeSink, frames <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-frames:
			if !ok {
				return
			}
			sink.Write(frame)
		}
	}
}

// ivfWriter 把 VP8 帧包成 IVF，方便 ffmpeg 从管道里读。
type ivfWriter struct {
	w      io.Writer
	ready  bool
	pts    uint64
	header bytes.Buffer
}

func (v *ivfWriter) WriteFrame(frame []byte) []byte {
	if !v.ready {
		width, height, ok := vp8Size(frame)
		if !ok {
			return nil
		}
		writeIVFHeader(&v.header, width, height)
		v.ready = true
	}
	var hdr [12]byte
	hdr[0] = byte(len(frame))
	hdr[1] = byte(len(frame) >> 8)
	hdr[2] = byte(len(frame) >> 16)
	hdr[3] = byte(len(frame) >> 24)
	for i := 0; i < 8; i++ {
		hdr[4+i] = byte(v.pts >> (8 * i))
	}
	v.pts++
	out := append(append([]byte{}, v.header.Bytes()...), hdr[:]...)
	v.header.Reset()
	return append(out, frame...)
}

func writeIVFHeader(w io.Writer, width, height int) {
	hdr := make([]byte, 32)
	copy(hdr[0:4], "DKIF")
	hdr[6] = 32
	copy(hdr[8:12], "VP80")
	hdr[12] = byte(width)
	hdr[13] = byte(width >> 8)
	hdr[14] = byte(height)
	hdr[15] = byte(height >> 8)
	hdr[16] = 30
	hdr[20] = 1
	_, _ = w.Write(hdr)
}

func vp8Size(frame []byte) (int, int, bool) {
	if len(frame) < 10 || frame[0]&1 != 0 {
		return 0, 0, false
	}
	if frame[3] != 0x9d || frame[4] != 0x01 || frame[5] != 0x2a {
		return 0, 0, false
	}
	width := int(frame[6]) | int(frame[7]&0x3f)<<8
	height := int(frame[8]) | int(frame[9]&0x3f)<<8
	return width, height, width > 0 && height > 0
}
