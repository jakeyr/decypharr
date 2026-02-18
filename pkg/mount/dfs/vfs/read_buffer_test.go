package vfs

import (
	"bytes"
	"testing"
)

func TestTailReadBufferSequential(t *testing.T) {
	buf := newTailReadBuffer(1024)
	if buf == nil {
		t.Fatal("expected buffer to be initialized")
	}

	src := []byte("abcdefghijklmnopqrstuvwxyz")
	buf.WriteAt(0, src)

	out := make([]byte, len(src))
	n, ok := buf.ReadAt(out, 0)
	if !ok || n != len(src) {
		t.Fatalf("expected full hit, ok=%v n=%d", ok, n)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("unexpected read data: got %q want %q", out, src)
	}
}

func TestTailReadBufferTrim(t *testing.T) {
	buf := newTailReadBuffer(8)
	src := []byte("0123456789abcdef")
	buf.WriteAt(0, src)

	want := []byte("89abcdef")
	out := make([]byte, len(want))
	n, ok := buf.ReadAt(out, 8)
	if !ok || n != len(want) {
		t.Fatalf("expected tail hit, ok=%v n=%d", ok, n)
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("unexpected tail data: got %q want %q", out, want)
	}
}

func TestTailReadBufferOutOfOrderReset(t *testing.T) {
	buf := newTailReadBuffer(16)
	buf.WriteAt(32, []byte("qrstuvwx"))
	buf.WriteAt(0, []byte("abcdefgh"))

	outOld := make([]byte, 4)
	if _, ok := buf.ReadAt(outOld, 32); ok {
		t.Fatal("expected old range miss after reset")
	}

	outNew := make([]byte, 8)
	n, ok := buf.ReadAt(outNew, 0)
	if !ok || n != len(outNew) {
		t.Fatalf("expected new range hit, ok=%v n=%d", ok, n)
	}
	if string(outNew) != "abcdefgh" {
		t.Fatalf("unexpected data: %q", outNew)
	}
}
