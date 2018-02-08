package zysql

import (
	"bufio"
	"os"
	"fmt"
	"path/filepath"
)

func OpenWriter(p string, prefix string, typ string, no int) *Writer {
	var suffix string
	if no > 0 {
		suffix = fmt.Sprintf("(%d)", no)
	}
	path := p + "/" + prefix + suffix + "." + typ
	var file *os.File
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		return OpenWriter(p, prefix, typ, no+1)
	} else {
		absPath, _ := filepath.Abs(path)
		file, _ = os.OpenFile(absPath, os.O_RDWR|os.O_CREATE, 0777)
		return &Writer{
			path:   absPath,
			file:   file,
			writer: bufio.NewWriter(file),
		}
	}
}

type Writer struct {
	path   string
	file   *os.File
	writer *bufio.Writer
}

func (w *Writer) Flush(s ...string) {
	for _, msg := range s {
		w.file.Write([]byte(msg + "\n"))
	}
}

func (w *Writer) Clear() {
	w.file.Close()
	os.Remove(w.path)
}

func (w *Writer) Rename(p string) {
	w.file.Close()
	os.Rename(w.path, p)
}

func (w *Writer) Close() {
	w.file.Close()
}
