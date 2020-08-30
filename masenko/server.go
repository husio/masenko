package masenko

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/husio/masenko/masenko/proto"
	"github.com/husio/masenko/masenko/webui"
	"github.com/husio/masenko/store"
)

type ServerConfiguration struct {
	StoreDir string
	// MaxWalSize is the maximum allowed WAL file size in bytes. Once
	// reached a cleanup process that creates a fresh WAL file is started.
	// Setting this value to 0 disables this functionality.
	MaxWALSize uint64
	ListenTCP  string
	ListenHTTP string
	Heartbeat  time.Duration
}

func StartServer(ctx context.Context, conf ServerConfiguration) (*server, error) {
	ctx, cancel := context.WithCancel(ctx)

	mcounter := NewMetricsCounter()

	logger := log.New(os.Stdout, "masenko: ", log.LUTC)

	queue, err := store.OpenMemStore(conf.StoreDir, conf.MaxWALSize, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open store: %w", err)
	}

	var listenConf net.ListenConfig
	ln, err := listenConf.Listen(ctx, "tcp", conf.ListenTCP)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("client interface: %w", err)
	}

	webui := &http.Server{
		BaseContext:    func(net.Listener) context.Context { return ctx },
		Addr:           conf.ListenHTTP,
		Handler:        webui.NewWebUI(queue, mcounter),
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		MaxHeaderBytes: 1e5,
	}

	s := server{
		queue: queue,
		webui: webui,
		cliui: ln,
		errc:  make(chan error, 1),
		stop:  cancel,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := webui.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				return
			}
			select {
			case s.errc <- fmt.Errorf("http interface: %w", err):
				cancel()
			case <-ctx.Done():
			}
			return
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			cli, err := ln.Accept()
			if err != nil {
				// https://golang.org/src/internal/poll/fd.go?h=ErrNetClosing
				if strings.HasSuffix(err.Error(), "use of closed network connection") {
					return
				}
				select {
				case s.errc <- fmt.Errorf("server accept: %w", err):
					cancel()
				case <-ctx.Done():
				}
				return
			}
			go proto.HandleClient(ctx, cli, queue, conf.Heartbeat, mcounter)
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		s.webui.Shutdown(context.Background())
		s.cliui.Close()
	}()

	return &s, nil
}

type server struct {
	queue store.Queue
	webui *http.Server
	cliui net.Listener

	wg   sync.WaitGroup
	errc chan error
	stop func()
}

func (s *server) Wait() {
	s.wg.Wait()
}

func (s *server) Close() error {
	s.stop()
	s.wg.Wait()

	select {
	case err := <-s.errc:
		return err
	default:
		return nil
	}
}
