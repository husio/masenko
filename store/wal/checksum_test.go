package wal

import (
	"bytes"
	"testing"
)

func TestChecksumReadWrite(t *testing.T) {
	var b bytes.Buffer

	payload := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	wr := NewChecksumWriter(&b)
	for i, p := range payload {
		n, err := wr.Write(p)
		if err != nil {
			t.Fatalf("%d: write: %s", i, err)
		}
		if want := len(p) + checksumHeaderSize; want != n {
			t.Fatalf("%d: want %d bytes written, got %d", i, want, n)
		}
	}

	rd := NewChecksumReader(bytes.NewReader(b.Bytes()))
	buf := make([]byte, 1024)
	for i, p := range payload {
		n, err := rd.Read(buf)
		if err != nil {
			t.Fatalf("%d: read: %s", i, err)
		}
		if n != len(p) {
			t.Fatalf("%d: want %d, got %d", i, len(p), n)
		}
		if !bytes.Equal(buf[:n], p) {
			t.Fatalf("%d: want %q, got %q", i, string(buf[:n]), string(p))
		}
	}
}
