package batcher

import (
	"errors"
	"sync"
	"time"
)

const (
	DefaultBatchSize     = 100
	DefaultFlushInterval = time.Second
)

type Option func(*Batcher)

func WithBatchSize(size int) func(*Batcher) {
	return func(b *Batcher) {
		b.BatchSize = size
	}
}

func WithFlushInterval(interval time.Duration) func(*Batcher) {
	return func(b *Batcher) {
		b.FlushInterval = interval
	}
}

type Flusher interface {
	Flush(batch []interface{}) (recuperate bool)
}

type Batcher struct {
	BatchSize     int
	FlushInterval time.Duration

	batch   []interface{}
	ticker  *time.Ticker
	flusher Flusher

	pool   sync.Pool
	mu     sync.Mutex
	cancel chan struct{}
	wg     sync.WaitGroup

	closed bool
}

func New(f Flusher, opts ...Option) *Batcher {
	if f == nil {
		panic(errors.New("flusher must be deffined"))
	}

	b := &Batcher{
		BatchSize:     DefaultBatchSize,
		FlushInterval: DefaultFlushInterval,
		mu:            sync.Mutex{},
		cancel:        make(chan struct{}),
		wg:            sync.WaitGroup{},
		flusher:       f,
	}

	for _, opt := range opts {
		opt(b)
	}

	b.batch = make([]interface{}, 0, b.BatchSize)
	b.ticker = time.NewTicker(b.FlushInterval)
	b.pool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, b.BatchSize)
		},
	}

	b.run()

	return b
}

func (b *Batcher) run() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.ticker.C:
				b.mu.Lock()
				if len(b.batch) > 0 {
					b.flush(b.batch)
					b.batch = b.newBatch()
				}
				b.mu.Unlock()
			case <-b.cancel:
				return
			}
		}
	}()
}

func (b *Batcher) Close() {
	b.ticker.Stop()
	close(b.cancel)

	b.mu.Lock()
	b.closed = true

	if len(b.batch) > 0 {
		b.flush(b.batch)
		b.batch = nil
	}
	b.mu.Unlock()

	b.wg.Wait()
}

func (b *Batcher) newBatch() []interface{} {
	return b.pool.Get().([]interface{})[:0]
}

func (b *Batcher) flush(batch []interface{}) {
	b.ticker.Reset(b.FlushInterval)
	if b.flusher.Flush(batch) {
		b.Recuperate(batch)
	}
}

func (b *Batcher) Put(v interface{}) {
	var batch []interface{}

	b.mu.Lock()
	// Exit if closed.
	if b.closed {
		b.mu.Unlock()
		return
	}

	b.batch = append(b.batch, v)
	if len(b.batch) == cap(b.batch) {
		batch, b.batch = b.batch, b.newBatch()
	}
	b.mu.Unlock()

	if batch != nil {
		b.flush(batch)
	}
}

func (b *Batcher) Recuperate(batch []interface{}) {
	b.pool.Put(batch)
}