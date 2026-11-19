package reolink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/baichuan"
)

type profileInput interface {
	Read(context.Context) (baichuan.MediaPacket, error)
	Err() error
	ProbeTalk(context.Context, uint8) (baichuan.TalkFormat, error)
	StartTalk(context.Context, uint8) (talkSession, error)
	Discovery() discoverySnapshot
	Abort() error
	Stats() baichuan.MediaStats
	Close() error
}

type profileOpener func(context.Context, *Camera, source) (profileInput, error)

const capabilityProbeTimeout = 2 * time.Second

type discoverySnapshot struct {
	device           baichuan.DeviceInfo
	capabilities     baichuan.Capabilities
	deviceStatus     string
	capabilityStatus string
}

func (p *Producer) probe(input profileInput) (mediaProbe, error) {
	ctx, cancel := context.WithTimeout(p.ctx, profileProbeTimeout)
	defer cancel()
	var probe mediaProbe
	for {
		videoReady := p.source.suppressVideo || p.pipeline.videoMedia != nil
		audioReady := p.source.suppressAudio || p.pipeline.audioMedia != nil
		if videoReady && audioReady {
			break
		}
		packet, err := input.Read(ctx)
		if err != nil {
			missing := "video and audio"
			if videoReady {
				missing = "audio"
			} else if audioReady {
				missing = "video"
			}
			return mediaProbe{}, fmt.Errorf("reolink: probe media waiting for %s: %w", missing, err)
		}
		if packet.Kind == baichuan.MediaDiscontinuity {
			probe.reset()
			videoResyncs, audioResyncs := p.pipeline.videoResyncs, p.pipeline.audioResyncs
			p.pipeline = mediaPipeline{epoch: p.pipeline.epoch,
				sharedClock: p.pipeline.sharedClock, videoOnly: p.pipeline.videoOnly,
				videoResyncs: videoResyncs, audioResyncs: audioResyncs}
			p.invalidateVideoRate()
			continue
		}
		p.record(packet)
		switch packet.Kind {
		case baichuan.MediaVideoI:
			if !p.source.suppressVideo && p.pipeline.videoMedia == nil {
				if err = p.pipeline.probeVideo(packet); err != nil {
					return mediaProbe{}, err
				}
			}
		case baichuan.MediaAAC, baichuan.MediaADPCM:
			if !p.source.suppressAudio && p.pipeline.audioMedia == nil {
				if packet.Kind == baichuan.MediaAAC {
					if _, _, err = audioPayload(packet); err != nil {
						continue
					}
				}
				if err = p.pipeline.configureAudio(packet); err != nil {
					continue
				}
			}
		}
		if p.retainPacket(packet) {
			if err = p.retainProbe(&probe, packet); err != nil {
				return mediaProbe{}, err
			}
		}
	}
	return probe, nil
}

func (p *Producer) probeTalk() (baichuan.TalkFormat, bool, error) {
	p.probeTalkMu.Lock()
	defer p.probeTalkMu.Unlock()
	if p.talkKnown {
		return p.talkFormat, p.talkSupported, nil
	}
	p.mu.Lock()
	input := p.input
	state := p.state
	terminal := p.terminal
	p.mu.Unlock()
	if input == nil || state != profileActive {
		if terminal == nil {
			terminal = fmt.Errorf("reolink: profile %s", state)
		}
		return baichuan.TalkFormat{}, false, terminal
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.sourceTimeout())
	format, err := input.ProbeTalk(ctx, p.key.Channel)
	cancel()
	if err != nil {
		var unsupported *baichuan.UnsupportedTalkError
		if !errors.As(err, &unsupported) {
			return baichuan.TalkFormat{}, false, fmt.Errorf("reolink: probe talkback: %w", err)
		}
		p.talkKnown = true
		return baichuan.TalkFormat{}, false, nil
	}
	p.talkKnown = true
	p.talkSupported = true
	p.talkFormat = format
	return format, true, nil
}

func (p *Producer) startTalk(ctx context.Context) (talkSession, error) {
	p.mu.Lock()
	input := p.input
	state := p.state
	terminal := p.terminal
	p.mu.Unlock()
	if input == nil || state != profileActive {
		if terminal == nil {
			terminal = fmt.Errorf("reolink: profile %s", state)
		}
		return nil, terminal
	}
	return input.StartTalk(ctx, p.key.Channel)
}

type baichuanInput struct {
	client    *baichuan.Client
	preview   *baichuan.Preview
	discovery discoverySnapshot
	once      sync.Once
	err       error

	// Set when the preview rides the camera's shared connection.
	registry    *Registry
	shared      *sharedClient
	closeCtx    context.Context
	cancelClose context.CancelFunc
}

func openBaichuan(ctx context.Context, camera *Camera, s source) (profileInput, error) {
	var registry *Registry
	if camera != nil {
		registry = camera.registry
	}
	if registry != nil && s.protocol == "tcp" && !s.dedicated {
		shared, err := registry.acquireClient(ctx, camera.key, s.remote, s.config)
		if err == nil {
			input, err := startInput(ctx, shared.client, s)
			if err == nil {
				input.registry, input.shared = registry, shared
				input.closeCtx, input.cancelClose = context.WithCancel(context.Background())
				return input, nil
			}
			registry.releaseClient(shared)
			if !errors.Is(err, baichuan.ErrPreviewActive) {
				return nil, err
			}
			// The same profile is already open on the shared connection.
		} else if !errors.Is(err, errNotShareable) {
			return nil, err
		}
	}

	if registry != nil {
		if err := registry.checkBackoff(camera.key); err != nil {
			return nil, err
		}
	}
	client, err := baichuan.Dial(ctx, s.config)
	if err != nil {
		return nil, err
	}
	err = client.Login(ctx)
	if registry != nil {
		registry.noteLogin(camera.key, s.remote, err)
	}
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	input, err := startInput(ctx, client, s)
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return input, nil
}

func startInput(ctx context.Context, client *baichuan.Client, s source) (*baichuanInput, error) {
	var discovery discoverySnapshot
	if s.preRecord == 0 {
		discovery = discover(ctx, client, s.channel)
	}
	preview, err := client.StartPreviewHistory(ctx, s.channel, s.stream, s.preRecord)
	if err != nil {
		return nil, err
	}
	return &baichuanInput{client: client, preview: preview, discovery: discovery}, nil
}

func discover(ctx context.Context, client *baichuan.Client, channel uint8) discoverySnapshot {
	ctx, cancel := context.WithTimeout(ctx, capabilityProbeTimeout)
	defer cancel()
	var device baichuan.DeviceInfo
	var capabilities baichuan.Capabilities
	var deviceErr, capabilityErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		device, deviceErr = client.DeviceInfo(ctx)
	}()
	go func() {
		defer wg.Done()
		capabilities, capabilityErr = client.Capabilities(ctx, channel)
	}()
	wg.Wait()
	return discoverySnapshot{
		device: device, capabilities: capabilities,
		deviceStatus: discoveryStatus(deviceErr), capabilityStatus: discoveryStatus(capabilityErr),
	}
}

func discoveryStatus(err error) string {
	if err == nil {
		return "available"
	}
	return errorClass(err)
}

func (i *baichuanInput) Discovery() discoverySnapshot {
	return i.discovery
}

func (i *baichuanInput) Stats() baichuan.MediaStats {
	return i.preview.Stats()
}

func (i *baichuanInput) Read(ctx context.Context) (baichuan.MediaPacket, error) {
	return i.preview.Read(ctx)
}

func (i *baichuanInput) Err() error {
	if err := i.client.Err(); err != nil {
		return err
	}
	if i.preview != nil {
		select {
		case <-i.preview.Done():
			return i.preview.Err()
		default:
		}
	}
	return nil
}

func (i *baichuanInput) ProbeTalk(ctx context.Context, channel uint8) (baichuan.TalkFormat, error) {
	return i.client.ProbeTalk(ctx, channel)
}

func (i *baichuanInput) StartTalk(ctx context.Context, channel uint8) (talkSession, error) {
	return i.client.StartTalk(ctx, channel)
}

func (i *baichuanInput) Close() error {
	i.once.Do(func() {
		if i.shared != nil {
			// Stop only this preview; the connection stays up for the camera's other sources.
			i.err = i.preview.CloseContext(i.closeCtx)
			i.cancelClose()
			i.registry.releaseClient(i.shared)
			return
		}
		i.err = errors.Join(i.preview.Close(), i.client.Close())
	})
	return i.err
}

func (i *baichuanInput) Abort() error {
	if i.shared != nil {
		// Interrupt an in-flight graceful stop without closing the shared connection.
		i.cancelClose()
		i.once.Do(func() {
			i.err = i.preview.Abort()
			i.registry.releaseClient(i.shared)
		})
		return i.err
	}
	// Interrupt an in-flight graceful close before waiting on its once barrier.
	err := i.client.Close()
	i.once.Do(func() {
		i.err = errors.Join(i.preview.Close(), err)
	})
	return i.err
}

type mediaProbe struct {
	packets []baichuan.MediaPacket
	bytes   int
}

func (p *mediaProbe) retain(packet baichuan.MediaPacket) error {
	if packet.Kind == baichuan.MediaVideoI {
		p.reset()
	}
	if len(packet.Data) > maxProbeBytes-p.bytes {
		return mediaErrorf("reolink: codec probe exceeds %d bytes", maxProbeBytes)
	}
	if len(p.packets) >= maxProbePackets {
		return mediaErrorf("reolink: codec probe exceeds %d packets", maxProbePackets)
	}
	packet.Data = append([]byte(nil), packet.Data...)
	p.packets = append(p.packets, packet)
	p.bytes += len(packet.Data)
	return nil
}

func (p *mediaProbe) reset() {
	clear(p.packets)
	p.packets = p.packets[:0]
	p.bytes = 0
}

func (p *mediaProbe) release() {
	p.reset()
	p.packets = nil
}
