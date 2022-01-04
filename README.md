# Batcher 

[![Go Reference](https://pkg.go.dev/github.com/gotidy/batcher.svg)](https://pkg.go.dev/github.com/gotidy/batcher) 
[![Go Report Card](https://goreportcard.com/badge/github.com/gotidy/batcher)](https://goreportcard.com/report/github.com/gotidy/batcher)

`batcher` collect a data and flush it if the batch is full or the interval is elapsed.

## Installation

`go get github.com/gotidy/batcher`

## Examples

```go
batch := New(flusher)
batch.Put("some data")
```

## Documentation

[Go Reference](https://pkg.go.dev/github.com/gotidy/batcher)

## License

[Apache 2.0](https://github.com/gotidy/batcher/blob/master/LICENSE)
