package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
A raw-text client for the Masenko server.

Input is read from stdin and send to the server. Response is written to stdout.
Exit code is non-zero in case of an error response.

Although it is possible to write multiple commands, only a single response is
expected.

Usage of %s:
`, os.Args[0])
		flag.PrintDefaults()
	}
	addrFl := flag.String("a", "localhost:12345", "Masenko server address.")
	flag.Parse()

	c, err := net.Dial("tcp", *addrFl)
	if err != nil {
		die("dial: %s\n", err)
	}
	defer c.Close()

	switch err := copyStdin(c); {
	case err == nil:
		// All good.
	case errors.Is(err, io.EOF):
		fmt.Fprintln(os.Stderr, "Stdin input required.")
		os.Exit(2)
	default:
		die("copy: %s\n", err)
	}

	var (
		isErr bool
		buf   = make([]byte, 1024)
	)
	for read := true; read; {
		n, err := c.Read(buf)
		if err != nil {
			if err != io.EOF {
				die("read: %s\n", err)
			}
			if n == 0 {
				return
			}
		}
		data := buf[:n]
		for i, c := range data {
			if c == '\n' {
				data = data[:i]
				read = false
				break
			}
		}
		if bytes.HasPrefix(data, []byte("ERR ")) {
			isErr = true
		}

		_, _ = os.Stdout.Write(data)
	}

	if isErr {
		os.Exit(3)
	} else {
		os.Exit(0)
	}
}

func die(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func copyStdin(dest io.Writer) error {
	// Check if the data is being piped. That should prevent us from
	// waiting for a data on a reader that no one ever writes to.
	if info, err := os.Stdin.Stat(); err == nil {
		isPipe := (info.Mode() & os.ModeCharDevice) == 0
		if !isPipe {
			return io.EOF
		}
	}
	if _, err := io.Copy(dest, os.Stdin); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return nil
}
