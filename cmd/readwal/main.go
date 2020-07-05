package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/husio/masenko/store/wal"
)

func main() {
	nx := wal.NewOpNexter(os.Stdin, 1e6)
	for {
		ops, err := nx.Next()
		switch {
		case err == nil:
			// All good.
		case errors.Is(err, io.EOF):
			return
		default:
			log.Fatalf("read WAL entry: %s", err)
		}

		for _, op := range ops {
			raw, err := json.Marshal(op)
			if err != nil {
				log.Fatalf("JSON marshal WAL operation: %s", err)
			}
			fmt.Println(string(raw))
		}
	}
}
