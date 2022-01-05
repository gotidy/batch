package batcher

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

const batchSize = 100

func Benchmark_No_Recuperate_Put(b *testing.B) {
	batch := New(
		FlusherNoRecuperateFunc(func(batch []interface{}) {
			// b.Logf("Batch: %+v", batch)
		}),
		WithBatchSize(batchSize),
		WithFlushInterval(time.Minute),
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch.Put("some data")
	}

	b.StopTimer()

	batch.Close()
}

func Benchmark_Put(b *testing.B) {
	batch := New(
		FlusherFunc(func(batch []interface{}) {
			// b.Logf("Batch: %+v", batch)
		}),
		WithBatchSize(batchSize),
		WithFlushInterval(time.Minute),
	)
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
	var data []int

	flusher := func(batch []interface{}) {
		for _, v := range batch {
			data = append(data, v.(int))
		}
	}

	batch := New(FlusherNoRecuperateFunc(flusher), WithBatchSize(10), WithFlushInterval(time.Second))

	var sample []int
	for i := 0; i < 30; i++ {
		batch.Put(i)
		sample = append(sample, i)
	}

	batch.Close()

	sort.Ints(data)

	if !reflect.DeepEqual(data, sample) {
		t.Errorf("expected %+v, got %+v", sample, data)
	}
}

func TestBatcherPeriodicalFlush(t *testing.T) {
	var data []int

	flusher := func(batch []interface{}) {
		for _, v := range batch {
			data = append(data, v.(int))
		}
	}

	batch := New(FlusherFunc(flusher), WithBatchSize(10), WithFlushInterval(time.Second))

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

	sort.Ints(data)

	if !reflect.DeepEqual(data, sample) {
		t.Errorf("expected %+v, got %+v", sample, data)
	}
}

func TestBatcherNewPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("must panic when flusher is nil")
		}
	}()
	_ = New(nil, WithBatchSize(10), WithFlushInterval(time.Second))
}

func TestBatcherPutToClosed(t *testing.T) {
	var data []int

	flusher := func(batch []interface{}) {
		for _, v := range batch {
			data = append(data, v.(int))
		}
	}

	batch := New(FlusherFunc(flusher), WithBatchSize(1), WithFlushInterval(time.Second))

	batch.Put(1)

	batch.Close()

	if !reflect.DeepEqual(data, []int{1}) {
		t.Errorf("expected %+v, got %+v", []int{1}, data)
	}

	data = nil

	batch.Put(2)

	if data != nil {
		t.Errorf("closed batcher must not accumulate data")
	}
}
