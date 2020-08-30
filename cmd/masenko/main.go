package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/husio/masenko/masenko"
)

// This variable is set during compilation time.
var GitCommit string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt)
		defer signal.Stop(sigc)
		select {
		case <-ctx.Done():
		case <-sigc:
			cancel()
		}
	}()

	conf := masenko.ServerConfiguration{
		StoreDir:         envStr("MASENKO_STORE_DIR", os.TempDir()),
		MaxWALSize:       uint64(envInt("MASENKO_MAX_WAL_SIZE", 25e6)), // Around 25Mb.
		ListenTCP:        envStr("MASENKO_LISTEN_TCP", ":12345"),
		ListenPrometheus: envStr("MASENKO_LISTEN_PROMETHEUS", ":1221"),
		Heartbeat:        envDur("MASENKO_HEARTBEAT", 45*time.Second),
	}

	server, err := masenko.StartServer(ctx, conf)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	server.Wait()

	if err := server.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
}

func envStr(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return fallback
}

func envDur(name string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(name); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Value %s is not a valid duration.", name)
			os.Exit(2)
		}
		return d
	}
	return fallback
}

func envInt(name string, fallback int) int {
	if v, ok := os.LookupEnv(name); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Value %s is not a valid number.", name)
			os.Exit(2)
		}
		return int(n)
	}
	return fallback

}
