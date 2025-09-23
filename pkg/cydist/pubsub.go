package cydist

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/redis/go-redis/v9"
)

// TaskHandler is the interface for handling tasks (duplicated from consume.go for Redis implementation)
type TaskHandler interface {
	ProcessTask(ctx context.Context, payload []byte) error
}

// HandlerFunc is a function type that implements TaskHandler (duplicated from consume.go for Redis implementation)
type HandlerFunc func(ctx context.Context, payload []byte) error

// ProcessTask calls the HandlerFunc
func (f HandlerFunc) ProcessTask(ctx context.Context, payload []byte) error {
	return f(ctx, payload)
}

// UnmarshalPayloadTyped unmarshals the payload into a specific type
func UnmarshalPayloadTyped[T any](payload []byte) (T, error) {
	var result T
	err := json.Unmarshal(payload, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal payload to specific type: %w", err)
	}

	return result, nil
}

// RedisPublisher represents a Redis pub/sub publisher
type RedisPublisher struct {
	client  *RedisClient
	ctx     context.Context
	channel string
}

// RedisConsumer represents a Redis pub/sub consumer
type RedisConsumer struct {
	client   *RedisClient
	pubsub   *redis.PubSub
	ctx      context.Context
	cancel   context.CancelFunc
	handlers map[string]TaskHandler
	wg       sync.WaitGroup
	channel  string
}

// NewRedisPublisher creates a new Redis publisher using application configuration
func NewRedisPublisher(redisCli *RedisClient, channel ...string) (*RedisPublisher, error) {
	if redisCli == nil {
		return nil, fmt.Errorf("redisCli cannot be nil")
	}
	ch := "msq_channel"
	if len(channel) > 0 {
		ch = channel[0]
	}
	return &RedisPublisher{
		client:  redisCli,
		ctx:     context.Background(),
		channel: ch,
	}, nil
}

// PublishSimple publishes a message to a Redis channel with default options
func (p *RedisPublisher) PublishSimple(channel string, payload interface{}) error {
	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Create message with type and payload
	message := struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}{
		Type:    channel, // Use channel as the type
		Payload: payloadBytes,
	}

	// Marshal the complete message
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Publish to Redis
	_, err = p.client.Publish(p.ctx, "msq_channel", messageBytes)
	if err != nil {
		return err
	}

	cylog.Debugf("Published message to channel msq_channel with type %s", channel)
	return nil
}

// Close closes the Redis client connection
func (p *RedisPublisher) Close() error {
	return p.client.Close()
}

// NewRedisConsumerWithConfig creates a new Redis consumer using application configuration
func NewRedisConsumer(redisCli *RedisClient, channel ...string) (*RedisConsumer, error) {
	if redisCli == nil {
		return nil, fmt.Errorf("redisCli cannot be nil")
	}
	ch := "msq_channel"
	if len(channel) > 0 {
		ch = channel[0]
	}
	// Subscribe to the channel
	pubsub := redisCli.universalClient.Subscribe(context.Background(), ch)

	return &RedisConsumer{
		client:   redisCli,
		pubsub:   pubsub,
		ctx:      context.Background(),
		cancel:   nil,
		handlers: make(map[string]TaskHandler),
		channel:  ch,
	}, nil
}

// RegisterHandler registers a handler for a specific message type
func (c *RedisConsumer) RegisterHandler(messageType string, handler TaskHandler) error {
	c.handlers[messageType] = handler
	return nil
}

// RegisterHandlerFunc registers a handler function for a specific message type
func (c *RedisConsumer) RegisterHandlerFunc(messageType string, handlerFunc func(ctx context.Context, payload []byte) error) error {
	c.RegisterHandler(messageType, HandlerFunc(handlerFunc))
	return nil
}

// Start starts the consumer to listen for messages
func (c *RedisConsumer) Start(ctx context.Context) error {
	if len(c.handlers) == 0 {
		return fmt.Errorf("no handlers registered")
	}

	// Start the message processing goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.processMessages(ctx)
	}()

	cylog.Info("Redis consumer started")
	return nil
}

// processMessages processes incoming messages from Redis pub/sub
func (c *RedisConsumer) processMessages(ctx context.Context) {
	ch := c.pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			cylog.Info("Redis consumer context cancelled, stopping message processing")
			return
		case msg, ok := <-ch:
			if !ok {
				cylog.Warn("Redis pub/sub channel closed")
				return
			}

			// Process the message in a separate goroutine
			go c.handleMessage(msg.Payload)
		}
	}
}

// handleMessage processes a single message
func (c *RedisConsumer) handleMessage(messageStr string) {
	// Parse the message
	var message struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}

	err := json.Unmarshal([]byte(messageStr), &message)
	if err != nil {
		cylog.Errorf("Failed to unmarshal message: %v", err)
		return
	}

	// Find the handler for this message type
	handler, exists := c.handlers[message.Type]
	if !exists {
		cylog.Warnf("No handler registered for message type: %s", message.Type)
		return
	}

	// No middleware handling in this implementation

	// Process the message
	start := time.Now()
	err = handler.ProcessTask(c.ctx, message.Payload)
	duration := time.Since(start)

	if err != nil {
		cylog.Errorf("Error processing message type %s: %v (took %v)", message.Type, err, duration)
	} else {
		cylog.Debugf("Successfully processed message type %s (took %v)", message.Type, duration)
	}
}

// Shutdown gracefully shuts down the consumer
func (c *RedisConsumer) Shutdown() {
	cylog.Info("Shutting down Redis consumer")

	// Close the pubsub connection
	if c.pubsub != nil {
		_ = c.pubsub.Close()
	}

	// Cancel the context to stop message processing
	c.cancel()

	// Wait for all goroutines to finish
	c.wg.Wait()

	// Close the Redis client
	if c.client != nil {
		_ = c.client.Close()
	}
	cylog.Info("Redis consumer shutdown complete")
}

// LocalMessage represents a message in the local pub/sub system
type LocalMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// LocalPublisher represents a local pub/sub publisher
type LocalPublisher struct {
	mu        sync.RWMutex
	listeners map[string][]chan LocalMessage
	channel   string
}

// NewLocalPublisher creates a new local publisher
func NewLocalPublisher(channel ...string) *LocalPublisher {
	ch := "msq_channel"
	if len(channel) > 0 {
		ch = channel[0]
	}
	return &LocalPublisher{
		listeners: make(map[string][]chan LocalMessage),
		channel:   ch,
	}
}

// PublishSimple publishes a message to a local channel
func (p *LocalPublisher) PublishSimple(channel string, payload interface{}) error {
	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Create message with type and payload
	message := LocalMessage{
		Type:    channel, // Use channel as the type
		Payload: payloadBytes,
	}

	p.mu.RLock()
	listeners := p.listeners[p.channel]
	p.mu.RUnlock()

	// Send to all listeners
	for _, listener := range listeners {
		select {
		case listener <- message:
			// Message sent successfully
		default:
			// Non-blocking send, skip if channel is full
			cylog.Warnf("Skipping message delivery to a slow consumer for channel %s", p.channel)
		}
	}

	cylog.Debugf("Published message to local channel %s with type %s", p.channel, channel)
	return nil
}

// Subscribe adds a listener for the specified channel
func (p *LocalPublisher) Subscribe(ch chan LocalMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners[p.channel] = append(p.listeners[p.channel], ch)
}

// Unsubscribe removes a listener for the specified channel
func (p *LocalPublisher) Unsubscribe(ch chan LocalMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	listeners := p.listeners[p.channel]
	for i, listener := range listeners {
		if listener == ch {
			p.listeners[p.channel] = append(listeners[:i], listeners[i+1:]...)
			break
		}
	}
}

// Close closes the local publisher
func (p *LocalPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear all listeners
	p.listeners = make(map[string][]chan LocalMessage)
	return nil
}

// LocalConsumer represents a local pub/sub consumer
type LocalConsumer struct {
	publisher *LocalPublisher
	ch        chan LocalMessage
	ctx       context.Context
	cancel    context.CancelFunc
	handlers  map[string]TaskHandler
	wg        sync.WaitGroup
	channel   string
}

// NewLocalConsumer creates a new local consumer
func NewLocalConsumer(publisher *LocalPublisher, channel ...string) *LocalConsumer {
	ch := "msq_channel"
	if len(channel) > 0 {
		ch = channel[0]
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalConsumer{
		publisher: publisher,
		ch:        make(chan LocalMessage, 100), // Buffer size of 100
		ctx:       ctx,
		cancel:    cancel,
		handlers:  make(map[string]TaskHandler),
		channel:   ch,
	}
}

// RegisterHandler registers a handler for a specific message type
func (c *LocalConsumer) RegisterHandler(messageType string, handler TaskHandler) error {
	c.handlers[messageType] = handler
	return nil
}

// RegisterHandlerFunc registers a handler function for a specific message type
func (c *LocalConsumer) RegisterHandlerFunc(messageType string, handlerFunc func(ctx context.Context, payload []byte) error) error {
	c.RegisterHandler(messageType, HandlerFunc(handlerFunc))
	return nil
}

// Start starts the consumer to listen for messages
func (c *LocalConsumer) Start(ctx context.Context) error {
	if len(c.handlers) == 0 {
		return fmt.Errorf("no handlers registered")
	}

	// Subscribe to the publisher
	c.publisher.Subscribe(c.ch)

	// Start the message processing goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.processMessages(ctx)
	}()

	cylog.Info("Local consumer started")
	return nil
}

// processMessages processes incoming messages from the local pub/sub
func (c *LocalConsumer) processMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			cylog.Info("Local consumer context cancelled, stopping message processing")
			return
		case msg := <-c.ch:
			// Process the message in a separate goroutine
			go c.handleMessage(msg)
		}
	}
}

// handleMessage processes a single message
func (c *LocalConsumer) handleMessage(message LocalMessage) {
	// Find the handler for this message type
	handler, exists := c.handlers[message.Type]
	if !exists {
		cylog.Warnf("No handler registered for message type: %s", message.Type)
		return
	}

	// Process the message
	start := time.Now()
	err := handler.ProcessTask(c.ctx, message.Payload)
	duration := time.Since(start)

	if err != nil {
		cylog.Errorf("Error processing message type %s: %v (took %v)", message.Type, err, duration)
	} else {
		cylog.Debugf("Successfully processed message type %s (took %v)", message.Type, duration)
	}
}

// Shutdown gracefully shuts down the consumer
func (c *LocalConsumer) Shutdown() {
	cylog.Info("Shutting down local consumer")

	// Unsubscribe from the publisher
	c.publisher.Unsubscribe(c.ch)

	// Cancel the context to stop message processing
	c.cancel()

	// Wait for all goroutines to finish
	c.wg.Wait()

	cylog.Info("Local consumer shutdown complete")
}

type Broadcaster struct {
	pub interface {
		PublishSimple(channel string, payload interface{}) error
		Close() error
	}
	sub interface {
		RegisterHandler(messageType string, handler TaskHandler) error
		RegisterHandlerFunc(messageType string, handlerFunc func(ctx context.Context, payload []byte) error) error
		Start(ctx context.Context) error
		Shutdown()
	}
	isLocal bool
}

type BroadcasterConfig struct {
	client  *RedisClient
	channel string
}
type BroadcasterOption func(*BroadcasterConfig)

func WithClient(redisCli *RedisClient) BroadcasterOption {
	return func(c *BroadcasterConfig) { c.client = redisCli }
}

func WithChannel(channel string) BroadcasterOption {
	return func(c *BroadcasterConfig) { c.channel = channel }
}

func NewBroadcaster(opts ...BroadcasterOption) (*Broadcaster, error) {
	config := &BroadcasterConfig{}
	for _, opt := range opts {
		opt(config)
	}
	ch := "msq_channel"
	if len(config.channel) > 0 {
		ch = config.channel
	}

	// If RedisClient is nil, fall back to local pub/sub
	if config.client == nil {
		cylog.Info("RedisClient is nil, falling back to local publish-subscribe mechanism")
		localPub := NewLocalPublisher(ch)
		localSub := NewLocalConsumer(localPub, ch)
		return &Broadcaster{
			pub:     localPub,
			sub:     localSub,
			isLocal: true,
		}, nil
	}

	// Otherwise use Redis pub/sub
	pub, err := NewRedisPublisher(config.client, ch)
	if err != nil {
		return nil, err
	}
	sub, err := NewRedisConsumer(config.client, ch)
	if err != nil {
		return nil, err
	}
	return &Broadcaster{
		pub:     pub,
		sub:     sub,
		isLocal: false,
	}, nil
}

func (b *Broadcaster) PublishSimple(channel string, payload interface{}) error {
	return b.pub.PublishSimple(channel, payload)
}

func (b *Broadcaster) Start(ctx context.Context) error {
	return b.sub.Start(ctx)
}

func (b *Broadcaster) Shutdown() {
	b.sub.Shutdown()
}

func (b *Broadcaster) RegisterHandler(messageType string, handler TaskHandler) error {
	return b.sub.RegisterHandler(messageType, handler)
}

func (b *Broadcaster) RegisterHandlerFunc(messageType string, handlerFunc func(ctx context.Context, payload []byte) error) error {
	return b.sub.RegisterHandlerFunc(messageType, handlerFunc)
}
