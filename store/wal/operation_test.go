package wal

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestOpReadWrite(t *testing.T) {
	now := time.Now()
	// Serialization is using UNIX seconds precision.
	now = now.Truncate(time.Second)

	batches := [][]Operation{
		{
			&OpAdd{
				ID:        124124,
				Name:      "a-name",
				Queue:     "a-queue",
				Payload:   []byte("qwertyuiop0987654321"),
				Deadqueue: "a-deadqueue",
				ExecuteAt: &now,
				Retry:     2,
				BlockedBy: []uint32{1942942492, 42, 924},
			},
		},
		{
			&OpAdd{
				ID:        124124,
				Name:      "b-name",
				Queue:     "b-queue",
				Payload:   []byte("qwertyuiop0987654321"),
				Deadqueue: "b-deadqueue",
				ExecuteAt: &now,
				Retry:     2,
			},
			&OpDelete{
				ID: 12994292,
			},
		},
		{
			&OpAdd{
				ID:   123,
				Name: "b-name",
			},
			&OpDelete{
				ID: 123,
			},
			&OpFail{
				ID: 124,
			},
		},
	}

	var buf bytes.Buffer

	oa := NewOpAppender(&buf, 1e5)
	for i, batch := range batches {
		if _, err := oa.Append(batch...); err != nil {
			t.Fatalf("%d: append: %s", i, err)
		}
	}

	nx := NewOpNexter(bytes.NewReader(buf.Bytes()), 1e5)
	for i, batch := range batches {
		ops, err := nx.Next()
		if err != nil {
			t.Fatalf("%d: next: %s", i, err)
		}
		if !reflect.DeepEqual(batch, ops) {
			logJSON(t, "batch", batch)
			logJSON(t, "ops", ops)
			t.Fatalf("unexpected next call result: got %d, want %d", len(ops), len(batch))
		}
	}
}

func logJSON(t testing.TB, name string, v interface{}) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		t.Fatalf("cannot marshal: %s", err)
	}
	t.Logf("%s: %s", name, string(b))
}

func BenchmarkOperationSerialization(b *testing.B) {
	cases := map[string]Operation{
		"add": &OpAdd{
			ID:        92844821,
			Queue:     "a-queue",
			Name:      "a-name",
			Payload:   []byte("qwertyuiop0987654321"),
			Deadqueue: "a-deadqueue",
			Retry:     6,
		},
		"del": &OpDelete{
			ID: 121491249,
		},
		"fail": &OpFail{
			ID: 121491249,
		},
	}

	for name, op := range cases {
		b.Run(name, func(b *testing.B) {
			buf := make([]byte, 1e5)

			b.Run("serialize", func(b *testing.B) {
				var (
					n   int
					err error
				)
				for i := 0; i < b.N; i++ {
					n, err = op.Serialize(buf)
					if err != nil {
						b.Fatal("serialize", err)
					}
				}
				buf = buf[:n]
			})

			b.Run("deserialize", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					op.Deserialize(buf)
				}
			})
		})
	}
}

func TestSerializeDeserializeAdd(t *testing.T) {
	cases := map[string]OpAdd{
		"only mandatory fields": {
			ID:    194129,
			Queue: "a-queue",
			Name:  "a-name",
		},
		"fully populated": {
			ID:        1097859,
			Queue:     "a-queue",
			Name:      "a-name",
			Payload:   []byte("qwertyuiop0987654321"),
			Deadqueue: "a-deadqueue",
			Retry:     6,
			BlockedBy: []uint32{292941, 122813883},
		},
	}

	for testName, op := range cases {
		t.Run(testName, func(t *testing.T) {
			b := make([]byte, 1e5)
			n, err := op.Serialize(b)
			if err != nil {
				t.Fatalf("serialize: %s", err)
			}
			b = b[:n]
			var cpy OpAdd
			cpy.Deserialize(b)
			if !reflect.DeepEqual(cpy, op) {
				t.Logf("want %+v", op)
				t.Logf("copy %+v", cpy)
				t.Fatal("data corruption")
			}
		})
	}
}

func TestSerializeDeserializeDelete(t *testing.T) {
	op := OpDelete{
		ID: 1097859,
	}
	b := make([]byte, 1e5)
	n, err := op.Serialize(b)
	if err != nil {
		t.Fatalf("serialize: %s", err)
	}
	b = b[:n]
	var cpy OpDelete
	cpy.Deserialize(b)
	if !reflect.DeepEqual(cpy, op) {
		t.Logf("want %+v", op)
		t.Logf("copy %+v", cpy)
		t.Fatal("data corruption")
	}
}

func TestSerializeDeserializeFail(t *testing.T) {
	op := OpFail{
		ID: 1097859,
	}
	b := make([]byte, 1e5)
	n, err := op.Serialize(b)
	if err != nil {
		t.Fatalf("serialize: %s", err)
	}
	b = b[:n]
	var cpy OpFail
	cpy.Deserialize(b)
	if !reflect.DeepEqual(cpy, op) {
		t.Logf("want %+v", op)
		t.Logf("copy %+v", cpy)
		t.Fatal("data corruption")
	}
}
