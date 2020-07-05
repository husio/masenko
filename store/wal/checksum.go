package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// Encoding used by this package.
var enc = binary.LittleEndian

// Header is 4 bytes of payload size and 4 bytes of payload checksum. Each
// written chunk is prefixed with a header of that size.
const checksumHeaderSize = 8

func NewChecksumWriter(w io.Writer) *ChecksumWriter {
	return &ChecksumWriter{wr: w}
}

type ChecksumWriter struct {
	headerBuf [checksumHeaderSize]byte
	wr        io.Writer
}

// Write writes given payload together with a checksum header. Total bytes
// written are rerturned, including payload header.
func (cw *ChecksumWriter) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}

	if len(payload) > math.MaxUint32-checksumHeaderSize {
		return 0, fmt.Errorf("payload too big: %w", ErrSize)
	}

	header := cw.headerBuf[:]
	enc.PutUint32(header[0:4], uint32(len(payload)))
	enc.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if n, err := cw.wr.Write(header); err != nil {
		return n, fmt.Errorf("write header: %w", err)
	} else if n != checksumHeaderSize {
		return n, fmt.Errorf("incomplete header write: %d bytes written", n)
	}

	n, err := cw.wr.Write(payload)
	if err != nil {
		return n + checksumHeaderSize, fmt.Errorf("write payload: %w", err)
	}
	if n != len(payload) {
		return n + checksumHeaderSize, fmt.Errorf("incomplete payload write: %d bytes written", n)
	}
	return n + checksumHeaderSize, nil
}

func NewChecksumReader(r io.Reader) *ChecksumReader {
	return &ChecksumReader{rd: r}
}

type ChecksumReader struct {
	headerBuf [checksumHeaderSize]byte
	rd        io.Reader
}

// Read reads a single chunk of data and validate its correctness with
// checksum. On success, size of the payload written to buffer is returned.
func (cr *ChecksumReader) Read(buf []byte) (int, error) {
	header := cr.headerBuf[:]

	if n, err := cr.rd.Read(header); err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	} else if n != len(header) {
		return 0, fmt.Errorf("incomplete header read: %d", n)
	}

	payloadSize := enc.Uint32(header[0:4])
	payloadChecksum := enc.Uint32(header[4:8])

	if payloadSize > uint32(len(buf)) {
		return 0, fmt.Errorf("buffer too small to read payload: %w", ErrSize)
	}

	if n, err := cr.rd.Read(buf[:payloadSize]); err != nil {
		return 0, fmt.Errorf("read payload: %w", err)
	} else if uint32(n) != payloadSize {
		return 0, fmt.Errorf("incomplete payload read: %d != %d", n, payloadSize)
	}

	if c := crc32.ChecksumIEEE(buf[:payloadSize]); c != payloadChecksum {
		return 0, errors.New("corrupted data: checksum missmatch")
	}
	return int(payloadSize), nil
}

var ErrSize = errors.New("size")
