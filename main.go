package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/turn/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

//go:embed web/*
var webFiles embed.FS

// 仅在用本机地址打开页面时使用，无线网络通话不走这里。
const (
	turnUser = "student"
	turnPass = "course"
	realm    = "course"
)

type status struct {
	ICE     string  `json:"ice"`
	Codec   string  `json:"codec"`
	In      uint64  `json:"inPackets"`
	Out     uint64  `json:"outFrames"`
	Source  string  `json:"source"`
	Send    string  `json:"send"`
	Preview bool    `json:"preview"`
	Level   int     `json:"level"`
	InFPS   float64 `json:"inFps"`
	InW     int     `json:"inWidth"`
	InH     int     `json:"inHeight"`
	InLost  uint64  `json:"inLost"`
	InLoss  float64 `json:"inLoss"`
	InKbps  float64 `json:"inKbps"`
	Target  int     `json:"targetKbps"`
	Mic     bool    `json:"mic"`
	Speaker bool    `json:"speaker"`
}

type liveCall struct {
	pc     *webrtc.PeerConnection
	cancel context.CancelFunc
	played sync.Once
}

var (
	videoTrack  *webrtc.TrackLocalStaticSample
	vp8Track    *webrtc.TrackLocalStaticSample
	audioTrack  *webrtc.TrackLocalStaticSample
	previewFile string
	openWindow  bool
	lanIP       string
	turnPort    string

	statsMu   sync.Mutex
	stats     status
	levelSeen bool

	callMu  sync.Mutex
	current *liveCall
	rootCtx context.Context

	captureSource string
)

func logf(format string, args ...any) { log.Printf(format, args...) }

func main() {
	source := flag.String("source", "camera", "画面来源：camera")
	window := flag.Bool("window", true, "在电脑上弹出手机画面窗口")
	httpAddr := flag.String("http", ":8080", "电脑本机页面地址")
	httpsAddr := flag.String("https", ":8443", "手机通过无线网络打开的 https 地址")
	turnAddr := flag.String("turn", ":3478", "中继端口")
	flag.Parse()
	captureSource = *source

	openWindow = *window
	if err := ensureFFmpeg(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	lanIP = lanIPv4()
	turnPort = portOf(*turnAddr)
	if lanIP == "" {
		fmt.Fprintln(os.Stderr, "没有找到电脑的局域网地址，手机将无法和电脑建立通路")
		os.Exit(1)
	}

	previewFile = filepath.Join(os.TempDir(), "course-call-preview.jpg")
	_ = os.Remove(previewFile)
	stats.Send = fmt.Sprintf("%d×%d", frameWidth, frameHeight)
	stats.ICE = "尚未通话"

	var err error
	videoTrack, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeH264,
		ClockRate:   90000,
		SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
	}, "video", "pc")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建发送轨道失败：%v\n", err)
		os.Exit(1)
	}
	vp8Track, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeVP8,
		ClockRate: 90000,
	}, "video", "pc-vp8")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 VP8 发送轨道失败：%v\n", err)
		os.Exit(1)
	}
	audioTrack, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}, "audio", "pc-audio")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建音频轨道失败：%v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rootCtx = ctx

	turnServer, err := startTurn(*turnAddr, lanIP)
	if err != nil {
		fmt.Fprintf(os.Stderr, "中继启动失败：%v\n", err)
		os.Exit(1)
	}
	defer turnServer.Close()

	webRoot, err := fs.Sub(webFiles, "web")
	if err != nil {
		fmt.Fprintf(os.Stderr, "页面文件缺失：%v\n", err)
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/config", handleConfig)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/call", handleCall)
	mux.HandleFunc("/recv-level", handleLevel)
	mux.HandleFunc("/preview.jpg", handlePreview)
	mux.HandleFunc("/audio", handleAudio)
	mux.HandleFunc("/codecs", handleCodecs)
	mux.Handle("/", noCache(http.FileServer(http.FS(webRoot))))

	httpLn, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "页面端口 %s 已被占用：%v\n", *httpAddr, err)
		os.Exit(1)
	}
	httpsLn, err := net.Listen("tcp", *httpsAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "https 端口 %s 已被占用：%v\n", *httpsAddr, err)
		os.Exit(1)
	}
	cert, err := newCert([]string{lanIP})
	if err != nil {
		fmt.Fprintf(os.Stderr, "生成证书失败：%v\n", err)
		os.Exit(1)
	}
	httpSrv := &http.Server{Handler: mux}
	httpsSrv := &http.Server{Handler: mux, TLSConfig: &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}}
	go func() { _ = httpSrv.Serve(httpLn) }()
	go func() { _ = httpsSrv.ServeTLS(httpsLn, "", "") }()
	printBanner(*httpAddr, *httpsAddr)

	<-ctx.Done()
	shut, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shut)
	_ = httpsSrv.Shutdown(shut)
}

func printBanner(httpAddr, httpsAddr string) {
	fmt.Printf(`视频通话平台已启动。服务器在这台电脑上。

手机和电脑连同一个无线网络，用浏览器打开：
  https://%s%s
  浏览器提示不安全时，选择继续访问。

电脑上看手机画面：窗口标题是 phone。
打不开窗口时，用电脑浏览器打开 http://127.0.0.1%s/desk.html

`, lanIP, httpsAddr, httpAddr)
}

func ensureFFmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return errors.New("没有找到 ffmpeg。macOS 可以用 brew install ffmpeg")
	}
	if _, err := exec.LookPath("ffplay"); err != nil {
		logf("没有找到 ffplay，电脑上不会弹窗口，请改看预览页")
		openWindow = false
	}
	out, err := exec.Command("ffmpeg", "-hide_banner", "-encoders").Output()
	if err != nil {
		return fmt.Errorf("检查 ffmpeg 编码器失败：%w", err)
	}
	if !strings.Contains(string(out), "libx264") {
		return errors.New("ffmpeg 没有 libx264，无法编码 H.264")
	}
	if !strings.Contains(string(out), "libvpx") {
		return errors.New("ffmpeg 没有 libvpx，无法编码 VP8")
	}
	if !strings.Contains(string(out), "libopus") {
		return errors.New("ffmpeg 没有 libopus，无法编码 Opus")
	}
	return nil
}

func startTurn(addr, ip string) (*turn.Server, error) {
	udpConn, err := net.ListenPacket("udp4", addr)
	if err != nil {
		return nil, err
	}
	tcpLn, err := net.Listen("tcp4", addr)
	if err != nil {
		udpConn.Close()
		return nil, err
	}
	key := turn.GenerateAuthKey(turnUser, realm, turnPass)
	relay := &turn.RelayAddressGeneratorStatic{RelayAddress: net.ParseIP(ip), Address: "0.0.0.0"}
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(username, gotRealm string, srcAddr net.Addr) ([]byte, bool) {
			if username == turnUser && gotRealm == realm {
				return key, true
			}
			logf("中继拒绝了 %s 从 %s 的认证", username, srcAddr)
			return nil, false
		},
		EventHandler: turn.EventHandler{
			OnAllocationCreated: func(srcAddr, _ net.Addr, protocol, username, _ string, relayAddr net.Addr, _ int) {
				logf("中继已分配给 %s，地址 %s", username, relayAddr)
			},
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn:            udpConn,
			RelayAddressGenerator: relay,
		}},
		ListenerConfigs: []turn.ListenerConfig{{
			Listener:              tcpLn,
			RelayAddressGenerator: relay,
		}},
	})
	if err != nil {
		udpConn.Close()
		tcpLn.Close()
		return nil, err
	}
	return server, nil
}

func newPeerConnection() (*webrtc.PeerConnection, error) {
	settings := webrtc.SettingEngine{}
	settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	want := net.ParseIP(lanIP)
	settings.SetInterfaceFilter(func(name string) bool {
		ifc, err := net.InterfaceByName(name)
		if err != nil {
			return false
		}
		addrs, _ := ifc.Addrs()
		for _, addr := range addrs {
			ipn, ok := addr.(*net.IPNet)
			if ok && ipn.IP.Equal(want) {
				return true
			}
		}
		return false
	})

	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	registry := &interceptor.Registry{}
	if err := installCongestion(mediaEngine, registry); err != nil {
		return nil, err
	}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, registry); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(registry),
		webrtc.WithSettingEngine(settings),
	)
	return api.NewPeerConnection(webrtc.Configuration{})
}

func handleCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "只接受 POST", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		SDP string `json:"sdp"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, map[string]string{"error": "通话描述读不出来"})
		return
	}
	answer, err := acceptOffer(req.SDP)
	if err != nil {
		logf("处理通话请求失败：%v", err)
		writeJSON(w, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]string{"sdp": answer})
}

func acceptOffer(sdp string) (string, error) {
	if strings.TrimSpace(sdp) == "" {
		return "", errors.New("收到的通话描述是空的")
	}
	logf("对方提供：%s", summarizeCodecs(sdp))
	parent := rootCtx
	if parent == nil {
		parent = context.Background()
	}
	pc, err := newPeerConnection()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(parent)
	call := &liveCall{pc: pc, cancel: cancel}
	failed := true
	defer func() {
		if failed {
			cancel()
			_ = pc.Close()
		}
	}()

	gathered := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(gathered) }) }
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			finish()
		}
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		logf("连接状态：%s", iceText(st))
		setICE(iceText(st))
		if st == webrtc.PeerConnectionStateFailed {
			logf("没有找到通路。请确认手机和电脑在同一个无线网络")
			cancel()
		}
		if st == webrtc.PeerConnectionStateClosed {
			cancel()
		}
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		switch track.Kind() {
		case webrtc.RTPCodecTypeAudio:
			logf("收到对方声音，编码是 %s", track.Codec().MimeType)
			go playRemoteAudio(ctx, track)
		case webrtc.RTPCodecTypeVideo:
			logf("收到对方视频，编码是 %s", track.Codec().MimeType)
			setCodec(track.Codec().MimeType)
			call.played.Do(func() { go playRemote(ctx, pc, track) })
		}
	})

	outgoing := videoTrack
	kind := "h264"
	if strings.Contains(sdp, "H264") {
		logf("使用 H.264 发送")
	} else if strings.Contains(sdp, "VP8") {
		outgoing = vp8Track
		kind = "vp8"
		logf("对方没有 H.264，改用 VP8 发送")
	}
	useEncoder(parent, kind, outgoing, captureSource)
	// 必须在读取对方描述之前放上发送轨道，应答才会和这一路视频双向共用。
	// 放到读取之后，这一路只剩接收，再另开一路发送，手机画面到不了电脑。
	videoTr, err := pc.AddTransceiverFromTrack(outgoing, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendrecv,
	})
	if err != nil {
		return "", err
	}
	if strings.Contains(sdp, "H264") {
		if err := videoTr.SetCodecPreferences(answerVideoCodecs()); err != nil {
			logf("限定 H.264 失败：%v", err)
		}
	}
	drainRTCP(videoTr.Sender())
	audioTr, err := pc.AddTransceiverFromTrack(audioTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendrecv,
	})
	if err != nil {
		return "", err
	}
	drainRTCP(audioTr.Sender())
	startMic(parent, audioTrack)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		logf("按 H.264 生成应答失败：%v", err)
		for _, tr := range pc.GetTransceivers() {
			if tr.Kind() == webrtc.RTPCodecTypeVideo {
				_ = tr.SetCodecPreferences(nil)
			}
		}
		answer, err = pc.CreateAnswer(nil)
		if err != nil {
			return "", err
		}
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	if pc.ICEGatheringState() == webrtc.ICEGatheringStateComplete {
		finish()
	}
	select {
	case <-gathered:
	case <-time.After(4 * time.Second):
		logf("候选地址还没收集完，先把已有的应答发回去")
	}
	local := pc.LocalDescription()
	if local == nil || local.SDP == "" {
		return "", errors.New("没有生成应答")
	}
	logf("协商编码：%s", summarizeCodecs(local.SDP))
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=send") || strings.HasPrefix(line, "a=recv") || strings.HasPrefix(line, "a=inactive") {
			logf("对方方向：%s", line)
		}
	}
	for _, line := range strings.Split(local.SDP, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=send") || strings.HasPrefix(line, "a=recv") || strings.HasPrefix(line, "a=inactive") {
			logf("应答方向：%s", line)
		}
	}
	setCall(call)
	failed = false
	return local.SDP, nil
}

func setCall(next *liveCall) {
	callMu.Lock()
	prev := current
	current = next
	callMu.Unlock()
	if prev != nil && prev != next {
		prev.cancel()
		_ = prev.pc.Close()
	}
}

func playRemote(ctx context.Context, pc *webrtc.PeerConnection, track *webrtc.TrackRemote) {
	mime := strings.ToLower(track.Codec().MimeType)
	format := "h264"
	var built *samplebuilder.SampleBuilder
	if strings.Contains(mime, "vp8") {
		format = "ivf"
		built = samplebuilder.New(400, &codecs.VP8Packet{}, track.Codec().ClockRate)
	} else if strings.Contains(mime, "h264") {
		built = samplebuilder.New(400, &codecs.H264Packet{}, track.Codec().ClockRate)
	} else {
		logf("暂时不能解码 %s", track.Codec().MimeType)
		return
	}
	sink, err := startDecoder(ctx, previewFile, openWindow, format)
	if err != nil {
		logf("解码器启动失败：%v", err)
		return
	}
	defer sink.Close()
	phoneVideo.reset()

	askKeyFrame := func() {
		_ = pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
	}
	askKeyFrame()
	var lastFrame atomic.Int64
	lastFrame.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, lastFrame.Load())) <= 2*time.Second {
					continue
				}
				askKeyFrame()
				lastFrame.Store(time.Now().UnixNano())
			}
		}
	}()

	frames := make(chan []byte, 180)
	go writeFrames(ctx, sink, frames)
	var ivf ivfWriter
	announced := false
	waitKey := true
	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if ctx.Err() == nil {
				logf("接收结束：%v", err)
			}
			return
		}
		addInPacket()
		phoneVideo.addPacket(pkt.SequenceNumber, len(pkt.Payload))
		built.Push(pkt)
		sample := built.Pop()
		if sample == nil {
			continue
		}
		key := false
		if format == "ivf" {
			_, _, key = vp8Size(sample.Data)
		} else {
			key = h264Key(sample.Data)
		}
		if sample.PrevDroppedPackets > 0 {
			waitKey = true
			askKeyFrame()
		}
		if waitKey && !key {
			continue
		}
		waitKey = false
		var payload []byte
		if format == "ivf" {
			payload = ivf.WriteFrame(sample.Data)
		} else {
			payload = append([]byte(nil), sample.Data...)
		}
		if len(payload) == 0 {
			continue
		}
		if format == "ivf" {
			w, h, _ := vp8Size(sample.Data)
			phoneVideo.addFrame(w, h)
		} else {
			phoneVideo.addFrame(0, 0)
		}
		lastFrame.Store(time.Now().UnixNano())
		select {
		case <-ctx.Done():
			return
		case frames <- payload:
		}
		if !announced {
			announced = true
			logf("已经收到对方的一帧，窗口或预览页可以看到")
		}
	}
}

func answerVideoCodecs() []webrtc.RTPCodecParameters {
	return []webrtc.RTPCodecParameters{
		{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeH264,
				ClockRate:   90000,
				SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			},
		},
		{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeVP8,
				ClockRate: 90000,
			},
		},
	}
}

func summarizeCodecs(sdp string) string {
	var lines []string
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=rtpmap:") && (strings.Contains(line, "H264") || strings.Contains(line, "VP8") || strings.Contains(line, "VP9") || strings.Contains(line, "opus")) {
			lines = append(lines, strings.TrimPrefix(line, "a=rtpmap:"))
		}
	}
	if len(lines) == 0 {
		return "没有视频编码"
	}
	return strings.Join(lines, "，")
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if name, _, err := net.SplitHostPort(r.Host); err == nil {
		host = name
	}
	relay := host == "127.0.0.1" || host == "localhost" || host == "::1"
	writeJSON(w, map[string]any{
		"relay":      relay,
		"turnURL":    "turn:127.0.0.1:" + turnPort + "?transport=tcp",
		"username":   turnUser,
		"credential": turnPass,
		"lanIP":      lanIP,
	})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	st := snapshot()
	if info, err := os.Stat(previewFile); err == nil && info.Size() > 0 {
		st.Preview = true
	}
	fps, kbps, loss, lost, wdt, hgt := phoneVideo.rates()
	st.InFPS = fps
	st.InKbps = kbps
	st.InLoss = loss
	st.InLost = lost
	st.InW = wdt
	st.InH = hgt
	st.Target = currentVideoBitrate() / 1000
	st.Mic = micOn.Load()
	st.Speaker = speakerOn.Load()
	writeJSON(w, st)
}

func handleCodecs(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8000))
	text := strings.TrimSpace(string(body))
	if text != "" {
		logf("手机编码能力：\n%s", text)
	}
	w.WriteHeader(http.StatusNoContent)
}

func handlePreview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, previewFile)
}

func handleLevel(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 32))
	n, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil {
		http.Error(w, "bad level", http.StatusBadRequest)
		return
	}
	setLevel(n)
	w.WriteHeader(http.StatusNoContent)
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func snapshot() status {
	statsMu.Lock()
	defer statsMu.Unlock()
	return stats
}

func setICE(v string) {
	statsMu.Lock()
	stats.ICE = v
	statsMu.Unlock()
}

func setCodec(v string) {
	statsMu.Lock()
	stats.Codec = v
	statsMu.Unlock()
}

func setSource(v string) {
	statsMu.Lock()
	stats.Source = v
	statsMu.Unlock()
}

func addOutFrame() {
	statsMu.Lock()
	stats.Out++
	statsMu.Unlock()
}

func addInPacket() {
	statsMu.Lock()
	stats.In++
	statsMu.Unlock()
}

// phoneVideo 统计电脑收到的手机视频。
var phoneVideo inboundMeter

type inboundMeter struct {
	mu                            sync.Mutex
	packets, lost, frames, nbytes uint64
	width, height                 int
	lastSeq                       uint16
	have                          bool
	marked                        time.Time
	markFrames, markBytes         uint64
	fps, kbps                     float64
	quiet                         int
}

func (m *inboundMeter) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.packets, m.lost, m.frames, m.nbytes = 0, 0, 0, 0
	m.width, m.height = 0, 0
	m.lastSeq = 0
	m.have = false
	m.marked = time.Time{}
	m.markFrames, m.markBytes = 0, 0
	m.fps, m.kbps = 0, 0
	m.quiet = 0
}

func (m *inboundMeter) addPacket(seq uint16, payload int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.have {
		d := int(seq) - int(m.lastSeq)
		if d < 0 {
			d += 65536
		}
		if d > 1 && d < 10000 {
			m.lost += uint64(d - 1)
		}
	}
	m.have = true
	m.lastSeq = seq
	m.packets++
	if payload > 0 {
		m.nbytes += uint64(payload)
	}
}

func (m *inboundMeter) addFrame(w, h int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frames++
	if w > 0 && h > 0 {
		m.width, m.height = w, h
	}
}

func (m *inboundMeter) setSize(w, h int) {
	if w <= 0 || h <= 0 {
		return
	}
	m.mu.Lock()
	m.width, m.height = w, h
	m.mu.Unlock()
}

func (m *inboundMeter) rates() (fps, kbps, loss float64, lost uint64, width, height int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if m.marked.IsZero() {
		m.marked = now
		m.markFrames, m.markBytes = m.frames, m.nbytes
	} else if dt := now.Sub(m.marked).Seconds(); dt >= 1 {
		instFPS := float64(m.frames-m.markFrames) / dt
		instKbps := float64(m.nbytes-m.markBytes) * 8 / 1000 / dt
		if instFPS == 0 && instKbps == 0 {
			m.quiet++
			if m.quiet >= 3 {
				m.fps, m.kbps = 0, 0
			}
		} else {
			m.quiet = 0
			if m.fps == 0 && m.kbps == 0 {
				m.fps, m.kbps = instFPS, instKbps
			} else {
				m.fps = m.fps*0.5 + instFPS*0.5
				m.kbps = m.kbps*0.5 + instKbps*0.5
			}
		}
		m.marked = now
		m.markFrames, m.markBytes = m.frames, m.nbytes
	}
	total := m.packets + m.lost
	if total > 0 {
		loss = float64(m.lost) / float64(total) * 100
	}
	return m.fps, m.kbps, loss, m.lost, m.width, m.height
}

func setLevel(n int) {
	statsMu.Lock()
	stats.Level = n
	already := levelSeen
	if n > 500 {
		levelSeen = true
	}
	statsMu.Unlock()
	if n > 500 && !already {
		logf("对方已经显示出电脑的画面")
	}
}

func iceText(st webrtc.PeerConnectionState) string {
	switch st {
	case webrtc.PeerConnectionStateNew:
		return "准备连通"
	case webrtc.PeerConnectionStateConnecting:
		return "正在连通"
	case webrtc.PeerConnectionStateConnected:
		return "通话中"
	case webrtc.PeerConnectionStateDisconnected:
		return "暂时断开"
	case webrtc.PeerConnectionStateFailed:
		return "连接失败"
	case webrtc.PeerConnectionStateClosed:
		return "已关闭"
	default:
		return st.String()
	}
}

func lanIPv4() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	fallback := ""
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := ifc.Addrs()
		for _, addr := range addrs {
			ipn, ok := addr.(*net.IPNet)
			if !ok || ipn.IP.To4() == nil {
				continue
			}
			ip := ipn.IP.To4()
			if ip[0] == 198 && ip[1] >= 18 && ip[1] <= 19 {
				continue
			}
			if ip[0] == 169 && ip[1] == 254 {
				continue
			}
			if ip[0] == 192 && ip[1] == 168 {
				return ip.String()
			}
			if fallback == "" {
				fallback = ip.String()
			}
		}
	}
	return fallback
}

func portOf(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.TrimPrefix(addr, ":")
	}
	return port
}

func newCert(ips []string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	addresses := []net.IP{net.ParseIP("127.0.0.1")}
	for _, ip := range ips {
		if parsed := net.ParseIP(ip); parsed != nil {
			addresses = append(addresses, parsed)
		}
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "course-call"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  addresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
