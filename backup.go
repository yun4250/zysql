package zysql

import (
	"bufio"
	"os"
	"fmt"
	"sync/atomic"
)

type Writer struct {
	path      string
	file      *os.File
	writer    *bufio.Writer
	semaphore int32
}

func (w *Writer) Flush(s ...string) <-chan error {
	atomic.AddInt32(&w.semaphore, 1)
	c := make(chan error, 1)
	go func() {
		defer atomic.AddInt32(&w.semaphore, -1)
		for _, msg := range s {
			fmt.Fprintln(w.writer, msg)
		}
		c <- w.writer.Flush()
	}()
	return c
}

func (w *Writer) Clear() {
	for atomic.AddInt32(&w.semaphore, 0) != 0 {}
	w.file.Close()
	os.Remove(w.path)
}

func (w *Writer) Close() {
	for atomic.AddInt32(&w.semaphore, 0) != 0 {}
	w.file.Close()
}
