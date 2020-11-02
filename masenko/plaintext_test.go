package masenko

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var goldFl = flag.Bool("gold", false, "Rebuild golden files.")

func TestPlainTextCases(t *testing.T) {
	paths, err := filepath.Glob("testdata/testcases/*.txt")
	if err != nil {
		t.Fatalf("test cases: %s", err)
	}
	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// List of connected clients is dynamic and new clients
			// should connect as soon as described by the test
			// file.
			type client struct {
				conn net.Conn

				wr io.Writer
				rd *bufio.Reader
			}
			clients := make(map[string]client)
			defer func() {
				for _, c := range clients {
					c.conn.Close()
				}
			}()

			clientByID := func(name string) client {
				if c, ok := clients[name]; ok {
					return c
				}

				for i := 0; i < 200; i++ {
					conn, err := net.Dial("tcp", "localhost:12001")
					if err == nil {
						c := client{conn: conn, wr: conn, rd: bufio.NewReader(conn)}
						clients[name] = c
						return c
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatal("cannot connect to the server")
				return client{}
			}

			fd, err := os.Open(p)
			if err != nil {
				t.Fatalf("open %q: %s", p, err)
			}
			defer fd.Close()

			server, err := StartServer(ctx, ServerConfiguration{
				StoreDir:         tempdir(t),
				ListenTCP:        "localhost:12001",
				ListenPrometheus: "localhost:12002",
				Heartbeat:        3 * time.Second,
			})
			if err != nil {
				t.Fatalf("start server: %s", err)
			}
			defer server.Close()

			var result bytes.Buffer
			testRd := bufio.NewReader(fd)
			var testLine int

		readTestIO:
			for {

				var (
					ignored []string
					line    string
				)
				for {
					line, err = testRd.ReadString('\n')
					testLine++
					switch {
					case errors.Is(err, nil):
						// All good.
					case errors.Is(err, io.EOF):
						break readTestIO
					default:
						t.Fatalf("read test request: %s", err)
					}
					if len(strings.TrimSpace(line)) != 0 && !strings.HasPrefix(line, "# ") {
						break
					}
					ignored = append(ignored, line)
				}

				chunks := strings.SplitN(line, " ", 3)
				if len(chunks) != 3 {
					t.Fatalf("line %d invalid: format must be CLIENT_ID ACTION CMD", testLine)
				}
				clientID, action, cmd := chunks[0], chunks[1], chunks[2]

				client := clientByID(clientID)
				switch action {
				case "send":
					if _, err := io.WriteString(client.wr, cmd); err != nil {
						t.Fatalf("line %d: client cannot write: %s", testLine, err)
					}
				case "recv":
					response, err := client.rd.ReadString('\n')
					if err != nil {
						t.Fatalf("line %d: client cannot read server response: %s", testLine, err)
					}
					if *goldFl {
						cmd = response
					} else {
						if response != cmd {
							t.Fatalf("line %d: unexpected respones\nwant %s\n got %s", testLine, cmd, response)
						}
					}
				default:
					t.Fatalf("line %d invalid: action must be 'send' or 'recv'", testLine)
				}

				if *goldFl {
					for _, line := range ignored {
						if _, err := result.WriteString(line); err != nil {
							t.Fatalf("cannot persist test result: %s", err)
						}
					}
					if _, err := fmt.Fprintf(&result, "%s %s %s", clientID, action, cmd); err != nil {
						t.Fatalf("cannot persist test result: %s", err)
					}
				}
			}
			if *goldFl {
				_ = fd.Close()
				if err := ioutil.WriteFile(p, result.Bytes(), 644); err != nil {
					t.Fatalf("write golden file: %s", err)
				}
			}
		})
	}
}

func slugify(s string) string {
	return strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(s, "_"), "_")
}

func tempdir(t testing.TB) string {
	t.Helper()

	dir, err := ioutil.TempDir(os.TempDir(), "masenko-tests-*")
	if err != nil {
		t.Fatalf("cannot create temporary directory: %s", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}
