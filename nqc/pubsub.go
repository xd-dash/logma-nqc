package nqc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Island struct {
	Name   string
	Client *redis.Client
}

type PublishResult struct {
	Island      string
	Subscribers int64
	Err         error
}

type FanoutPublisher struct {
	Namespace string
	Islands   []Island
}

func (p *FanoutPublisher) Publish(ctx context.Context, h Hint) []PublishResult {
	results := make([]PublishResult, len(p.Islands))
	state, err := h.Validate(p.Namespace, 4096)
	if err != nil {
		for i, island := range p.Islands {
			results[i] = PublishResult{Island: island.Name, Err: err}
		}
		return results
	}
	_ = state
	payload, err := h.Marshal()
	if err != nil {
		for i, island := range p.Islands {
			results[i] = PublishResult{Island: island.Name, Err: err}
		}
		return results
	}
	channel, err := UpdateChannel(p.Namespace)
	if err != nil {
		for i, island := range p.Islands {
			results[i] = PublishResult{Island: island.Name, Err: err}
		}
		return results
	}

	var wg sync.WaitGroup
	for i := range p.Islands {
		i := i
		island := p.Islands[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].Island = island.Name
			if island.Client == nil {
				results[i].Err = errors.New("nqc: nil island redis client")
				return
			}
			n, err := island.Client.Publish(ctx, channel, payload).Result()
			results[i].Subscribers = n
			if err != nil {
				results[i].Err = fmt.Errorf("nqc: publish to island %s: %w", island.Name, err)
			}
		}()
	}
	wg.Wait()
	return results
}

type SubscriberConfig struct {
	HintTTL         time.Duration
	QueueSize       int
	Workers         int
	MaxPayloadBytes int
	MaxKeyBytes     int
}

type SubscriberHooks struct {
	OnSubscribed func()
	OnApplied    func(Hint, bool)
	OnInvalid    func(error)
	OnDropped    func()
	OnError      func(error)
}

type Subscriber struct {
	client *redis.Client
	store  *RedisStore
	cfg    SubscriberConfig
	hooks  SubscriberHooks
}

func NewSubscriber(client *redis.Client, store *RedisStore, cfg SubscriberConfig, hooks SubscriberHooks) (*Subscriber, error) {
	if client == nil || store == nil {
		return nil, errors.New("nqc: subscriber requires redis client and store")
	}
	if cfg.HintTTL <= 0 {
		cfg.HintTTL = 10 * time.Minute
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = 16 << 10
	}
	if cfg.MaxKeyBytes <= 0 {
		cfg.MaxKeyBytes = 4096
	}
	return &Subscriber{client: client, store: store, cfg: cfg, hooks: hooks}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	channel, err := UpdateChannel(s.store.Namespace())
	if err != nil {
		return err
	}
	pubsub := s.client.Subscribe(ctx, channel)
	defer pubsub.Close()

	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = pubsub.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)

	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("nqc: subscribe %s: %w", channel, err)
	}
	if s.hooks.OnSubscribed != nil {
		s.hooks.OnSubscribed()
	}

	queue := make(chan string, s.cfg.QueueSize)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for payload := range queue {
				s.handlePayload(ctx, payload)
			}
		}()
	}
	defer func() {
		close(queue)
		wg.Wait()
	}()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if s.hooks.OnError != nil {
				s.hooks.OnError(err)
			}
			continue
		}
		select {
		case queue <- msg.Payload:
		default:
			if s.hooks.OnDropped != nil {
				s.hooks.OnDropped()
			}
		}
	}
}

func (s *Subscriber) handlePayload(ctx context.Context, payload string) {
	h, err := DecodeHint(payload, s.cfg.MaxPayloadBytes)
	if err != nil {
		if s.hooks.OnInvalid != nil {
			s.hooks.OnInvalid(err)
		}
		return
	}
	state, err := h.Validate(s.store.Namespace(), s.cfg.MaxKeyBytes)
	if err != nil {
		if s.hooks.OnInvalid != nil {
			s.hooks.OnInvalid(err)
		}
		return
	}
	advanced, err := s.store.AdvanceHint(ctx, h.Key, state, s.cfg.HintTTL)
	if err != nil {
		if s.hooks.OnError != nil {
			s.hooks.OnError(err)
		}
		return
	}
	if s.hooks.OnApplied != nil {
		s.hooks.OnApplied(h, advanced)
	}
}
