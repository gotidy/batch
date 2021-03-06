# Batch

[![Go Reference](https://pkg.go.dev/badge/github.com/gotidy/batch.svg)](https://pkg.go.dev/github.com/gotidy/batch) 
[![Go Report Card](https://goreportcard.com/badge/github.com/gotidy/batch)](https://goreportcard.com/report/github.com/gotidy/batch)

`Batcher` collect a data and flush it if the batch is full or the interval is elapsed.

## Installation

`go get github.com/gotidy/batch`

## Examples

```go
batch := New(flusher)
batch.Put("some data")
```

## Documentation

[Go Reference](https://pkg.go.dev/github.com/gotidy/batch)

## License

[Apache 2.0](https://github.com/gotidy/batch/blob/master/LICENSE)
