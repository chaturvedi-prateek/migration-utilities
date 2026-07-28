package main

import (
	"io"
	"os"
)

// multiWriter fans writes out to several writers (console + log file).
func multiWriter(ws ...io.Writer) io.Writer { return io.MultiWriter(ws...) }

// teeToFile redirects the process's stdout through a tee into path so every logf
// line is also captured in harness.log. Returns a close func that restores stdout.
func teeToFile(path string) (io.Writer, func()) {
	f, err := os.Create(path)
	if err != nil {
		return os.Stdout, func() {}
	}
	r, w, err := os.Pipe()
	if err != nil {
		f.Close()
		return os.Stdout, func() {}
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.MultiWriter(orig, f), r)
		close(done)
	}()
	return f, func() {
		_ = w.Close()
		<-done
		os.Stdout = orig
		_ = f.Close()
	}
}
