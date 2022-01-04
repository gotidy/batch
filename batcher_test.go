package batcher

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

const count = 100

type Flush struct {
	b          *testing.B
	recuperate bool
}

func (f Flush) Flush(batch []interface{}) (recuperate bool) {
	// f.b.Logf("Batch: %+v", batch)
	return f.recuperate
}

func Benchmark_No_Recuperate_Put(b *testing.B) {
	batch := New(Flush{b: b})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch.Put("some data")
	}

	b.StopTimer()

	batch.Close()
}

func Benchmark_Put(b *testing.B) {
	batch := New(Flush{b: b, recuperate: true})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch.Put("some data")
	}

	b.StopTimer()
	batch.Close()
}

type TestFlush struct {
	Data []int
}

func (f *TestFlush) Flush(batch []interface{}) (recuperate bool) {
	for _, v := range batch {
		f.Data = append(f.Data, v.(int))
	}
	return true
}

func TestBatcher(t *testing.T) {
	flusher := &TestFlush{}
	batch := New(flusher, WithBatchSize(10))

	var sample []int
	for i := 0; i < 30; i++ {
		batch.Put(i)
		sample = append(sample, i)
	}

	batch.Close()

	sort.Ints(flusher.Data)

	if !reflect.DeepEqual(flusher.Data, sample) {
		t.Errorf("expected %+v, got %+v", sample, flusher.Data)
	}
}

func TestBatcherPeriodicalFlush(t *testing.T) {
	flusher := &TestFlush{}
	batch := New(flusher, WithBatchSize(10), WithFlushInterval(time.Second))

	var sample []int

	for i := 0; i < 5; i++ {
		batch.Put(i)
		sample = append(sample, i)
	}

	time.Sleep(time.Second * 5)

	for i := 5; i < 10; i++ {
		batch.Put(i)
		sample = append(sample, i)
	}

	batch.Close()

	sort.Ints(flusher.Data)

	if !reflect.DeepEqual(flusher.Data, sample) {
		t.Errorf("expected %+v, got %+v", sample, flusher.Data)
	}
}
