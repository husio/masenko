package wal

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type OpAppender struct {
	cw  *ChecksumWriter
	buf []byte
}

// NewOpAppender returns an OpAppender that writes serialized operations into given
// writer in a single call.
func NewOpAppender(w io.Writer, bufsize int) *OpAppender {
	return &OpAppender{
		cw:  NewChecksumWriter(w),
		buf: make([]byte, bufsize),
	}
}

func (ow *OpAppender) Append(operations ...Operation) (int, error) {
	if len(operations) == 0 {
		return 0, nil
	}

	var totalSize int

	buf := ow.buf[:]
	enc.PutUint16(buf[:2], uint16(len(operations)))
	buf = buf[2:]
	totalSize += 2

	for _, op := range operations {
		if len(buf) < 3 {
			return 0, fmt.Errorf("buffer too small: %w", ErrSize)
		}
		n, err := op.Serialize(buf[2:])
		if err != nil {
			return 0, fmt.Errorf("serialize operation: %w", err)
		}
		enc.PutUint16(buf[:2], uint16(n))
		buf = buf[2+n:]
		totalSize += 2 + n
	}

	n, err := ow.cw.Write(ow.buf[:totalSize])
	if err != nil {
		return n, fmt.Errorf("write: %w", err)
	}
	return n, nil
}

type OpNexter struct {
	cr  *ChecksumReader
	buf []byte
}

func NewOpNexter(r io.Reader, bufsize int) *OpNexter {
	return &OpNexter{
		cr:  NewChecksumReader(r),
		buf: make([]byte, bufsize),
	}
}

func (or *OpNexter) Next() ([]Operation, error) {
	buf := or.buf[:]

	n, err := or.cr.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	buf = buf[:n]

	// Very little validation is needed, because read bytes are expected to
	// be a valid length. This must be ensured by the writer.

	amount := int(enc.Uint16(buf[:2]))
	buf = buf[2:]

	operations := make([]Operation, amount)
	for i := range operations {
		size := int(enc.Uint16(buf[:2]))
		op, err := DeserializeOperation(buf[2 : 2+size])
		if err != nil {
			return nil, fmt.Errorf("deseialize operation: %w", err)
		}
		operations[i] = op
		buf = buf[2+size:]
	}
	return operations, nil
}

type OperationKind uint8

const (
	opKindAdd = 1 << iota
	opKindDelete
	opKindFail
)

type Operation interface {
	Serialize([]byte) (int, error)
	Deserialize([]byte) error
}

func DeserializeOperation(b []byte) (Operation, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("buffer too small to contain operation kind: %w", ErrSize)
	}
	newOp := operationsIdx[b[0]]
	if newOp == nil {
		return nil, fmt.Errorf("unknown operation type %d: %w", b[0], ErrInput)
	}
	op := newOp()
	if err := op.Deserialize(b); err != nil {
		return nil, fmt.Errorf("deserailize %T: %w", op, err)
	}
	return op, nil
}

// operationsIdx is an index containing each operation accesible by it's kind.
// This is a must for the generic deserialization function.
var operationsIdx = [math.MaxUint8]func() Operation{
	opKindAdd:    func() Operation { return &OpAdd{} },
	opKindDelete: func() Operation { return &OpDelete{} },
	opKindFail:   func() Operation { return &OpFail{} },
}

type OpAdd struct {
	ID        uint32
	Queue     string
	Name      string
	Payload   []byte
	Deadqueue string
	ExecuteAt *time.Time
	Retry     uint8
	BlockedBy []uint32
}

func (op OpAdd) Serialize(b []byte) (int, error) {
	if len(op.Queue) > math.MaxUint8 {
		return 0, fmt.Errorf("%w: queue name too long", ErrInput)
	}
	if len(op.Name) > math.MaxUint8 {
		return 0, fmt.Errorf("%w: task name too long", ErrInput)
	}
	if len(op.Deadqueue) > math.MaxUint8 {
		return 0, fmt.Errorf("%w: deadqueue name too long", ErrInput)
	}
	if len(op.Payload) > math.MaxUint16 {
		return 0, fmt.Errorf("%w: arguments too big", ErrInput)
	}

	size := 1 + // Kind
		4 + // ID
		1 + len(op.Queue) +
		1 + len(op.Name) +
		1 + len(op.Deadqueue) +
		2 + len(op.Payload) +
		4 + // ExecuteAt as UNIX time
		1 + // Retry
		1 + 4*len(op.BlockedBy)
	if size > len(b) {
		return 0, fmt.Errorf("buffer too small: %w", ErrSize)
	}

	b[0] = opKindAdd
	b = b[1:]

	enc.PutUint32(b, op.ID)
	b = b[4:]

	b[0] = uint8(len(op.Queue))
	for i, c := range op.Queue {
		b[1+i] = byte(c)
	}
	b = b[1+len(op.Queue):]

	b[0] = uint8(len(op.Name))
	for i, c := range op.Name {
		b[1+i] = byte(c)
	}
	b = b[1+len(op.Name):]

	b[0] = uint8(len(op.Deadqueue))
	for i, c := range op.Deadqueue {
		b[1+i] = byte(c)
	}
	b = b[1+len(op.Deadqueue):]

	enc.PutUint16(b, uint16(len(op.Payload)))
	for i, c := range op.Payload {
		b[2+i] = c
	}
	b = b[2+len(op.Payload):]

	if op.ExecuteAt == nil {
		enc.PutUint32(b, 0)
	} else {
		enc.PutUint32(b, uint32(op.ExecuteAt.Unix()))
	}
	b = b[4:]

	b[0] = uint8(op.Retry)
	b = b[1:]

	b[0] = uint8(len(op.BlockedBy))
	for i, id := range op.BlockedBy {
		enc.PutUint32(b[1+i*4:], id)
	}
	b = b[1+len(op.BlockedBy)*4:]

	return size, nil
}

func (op *OpAdd) Deserialize(b []byte) error {
	if kind := OperationKind(b[0]); kind != opKindAdd {
		return fmt.Errorf("invalid kind %d: %w", kind, ErrInput)
	}
	b = b[1:]

	op.ID = enc.Uint32(b)
	b = b[4:]

	qsize := uint8(b[0])
	op.Queue = string(b[1 : 1+qsize])
	b = b[1+qsize:]

	nsize := uint8(b[0])
	op.Name = string(b[1 : 1+nsize])
	b = b[1+nsize:]

	dqsize := uint8(b[0])
	op.Deadqueue = string(b[1 : 1+dqsize])
	b = b[1+dqsize:]

	asize := enc.Uint16(b)
	if asize > 0 {
		op.Payload = make([]byte, asize)
		copy(op.Payload, b[2:2+asize])
	}
	b = b[2+asize:]

	if at := enc.Uint32(b[:4]); at == 0 {
		op.ExecuteAt = nil
	} else {
		executeAt := time.Unix(int64(at), 0)
		op.ExecuteAt = &executeAt
	}
	b = b[4:]

	op.Retry = uint8(b[0])
	b = b[1:]

	blockedLen := int(b[0])
	b = b[1:]
	if blockedLen > 0 {
		op.BlockedBy = make([]uint32, blockedLen)
		for i := 0; i < blockedLen; i++ {
			op.BlockedBy[i] = enc.Uint32(b)
			b = b[4:]
		}
	}

	return nil
}

type OpDelete struct {
	ID uint32
}

func (op OpDelete) Serialize(b []byte) (int, error) {
	const size = 5
	if len(b) < size {
		return 0, fmt.Errorf("buffer too small: %w", ErrSize)
	}
	b[0] = opKindDelete
	b = b[1:]
	enc.PutUint32(b, op.ID)
	return size, nil
}

func (op *OpDelete) Deserialize(b []byte) error {
	if kind := OperationKind(b[0]); kind != opKindDelete {
		return fmt.Errorf("invalid kind %d: %w", kind, ErrInput)
	}
	if len(b) < 5 {
		return fmt.Errorf("buffer too small: %w", ErrSize)
	}
	op.ID = enc.Uint32(b[1:])
	return nil
}

type OpFail struct {
	ID uint32
}

func (op OpFail) Serialize(b []byte) (int, error) {
	const size = 5
	if len(b) < size {
		return 0, fmt.Errorf("buffer too small: %w", ErrSize)
	}
	b[0] = opKindFail
	b = b[1:]
	enc.PutUint32(b, op.ID)
	return size, nil

}

func (op *OpFail) Deserialize(b []byte) error {
	if kind := OperationKind(b[0]); kind != opKindFail {
		return fmt.Errorf("invalid kind %d: %w", kind, ErrInput)
	}
	if len(b) < 5 {
		return fmt.Errorf("buffer too small: %w", ErrSize)
	}
	op.ID = enc.Uint32(b[1:])
	return nil
}

var ErrInput = errors.New("input")
