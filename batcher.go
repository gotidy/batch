package batch

import (
	"errors"
	"sync"
	"time"
)

const (
	// DefaultBatchSize is the default batch size.
	DefaultBatchSize = 100
	// DefaultFlushInterval is the default flush interval.
	DefaultFlushInterval = time.Second
)

// Flusher describes the interface of the batch flusher.
type Flusher interface {
	Flush(batch []interface{}) (recuperate bool)
}

// FlusherFunc implements the Flusher interface for the flushing function.
// Batch slice is recuperated automatically after returning.
type FlusherFunc func(batch []interface{})

// Flush the batch.
func (f FlusherFunc) Flush(batch []interface{}) (recuperate bool) {
	f(batch)
	return true
}

// FlusherNoRecuperateFunc implements the Flusher interface for the flushing function.
// The batch slice will not be recuperated. However it can be recuperated later with
// Batcher.Recuperate function.
type FlusherNoRecuperateFunc func(batch []interface{})

// Flush the batch.
func (f FlusherNoRecuperateFunc) Flush(batch []interface{}) (recuperate bool) {
	f(batch)
	return false
}

// Option sets the batcher option.
type Option func(*Batcher)

// WithBatchSize sets batch size.
func WithBatchSize(size int) func(*Batcher) {
	return func(b *Batcher) {
		b.BatchSize = size
	}
}

// WithFlushInterval sets flush interval.
func WithFlushInterval(interval time.Duration) func(*Batcher) {
	return func(b *Batcher) {
		b.FlushInterval = interval
	}
}

// Batcher accumulate messages in an internal buffer and flushes it if it is full
// or flush interval is expired.
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

// New creates a new batcher with defined Flusher and options.
// If Flusher is nil, then Batcher panic.
func New(f Flusher, opts ...Option) *Batcher {
	if f == nil {
		panic(errors.New("flusher must be defined"))
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

// Close the batcher.
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

// Put accumulate value into internal buffer.
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

// Recuperate the a batch slice fo reusing.
// Recuperation extremely decrease memory allocations.
func (b *Batcher) Recuperate(batch []interface{}) {
	b.pool.Put(batch)
}
