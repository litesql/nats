package extension

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/walterwanderley/sqlite"
)

type JetStreamPublisherVirtualTable struct {
	name         string
	js           jetstream.JetStream
	timeout      time.Duration
	logger       *slog.Logger
	loggerCloser io.Closer
}

func NewJetStreamPublisherVirtualTable(name string, servers string, opts []nats.Option, timeout time.Duration, loggerDef string) (*JetStreamPublisherVirtualTable, error) {
	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}

	var js jetstream.JetStream
	if servers != "" {
		opts = append(opts,
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				logger.Error("Got disconnected!", "reason", err)
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				logger.Info("Got reconnected!", "url", nc.ConnectedUrl())
			}),
			nats.ClosedHandler(func(nc *nats.Conn) {
				logger.Error("Connection closed.", "reason", nc.LastError())
			}))
		nc, err := nats.Connect(servers, opts...)
		if err != nil {
			if loggerCloser != nil {
				loggerCloser.Close()
			}
			return nil, fmt.Errorf("failed to connect to nats: %w", err)
		}
		js, err = jetstream.New(nc)
		if err != nil {
			if loggerCloser != nil {
				loggerCloser.Close()
			}
			return nil, fmt.Errorf("failed to connect to jetstream: %w", err)
		}
	}

	return &JetStreamPublisherVirtualTable{
		name:         name,
		js:           js,
		timeout:      timeout,
		logger:       logger,
		loggerCloser: loggerCloser,
	}, nil
}

func (vt *JetStreamPublisherVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{}, nil
}

func (vt *JetStreamPublisherVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return nil, fmt.Errorf("SELECT operations on %q are not supported", vt.name)
}

func (vt *JetStreamPublisherVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	if vt.js != nil {
		vt.js.Conn().Close()
	}
	return err
}

func (vt *JetStreamPublisherVirtualTable) Destroy() error {
	return nil
}

func (vt *JetStreamPublisherVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	subject := values[0].Text()
	if subject == "" {
		return 0, fmt.Errorf("subject is required")
	}
	data := values[1].Text()
	var (
		ctx    = context.Background()
		cancel func()
	)
	if vt.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, vt.timeout)
		defer cancel()
	}
	_, err := vt.js.Publish(ctx, subject, []byte(data))
	if err != nil {
		return 0, fmt.Errorf("failed to publish message: %w", err)
	}
	return 0, nil
}

func (vt *JetStreamPublisherVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.name)
}

func (vt *JetStreamPublisherVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.name)
}

func (vt *JetStreamPublisherVirtualTable) Delete(_ sqlite.Value) error {
	return fmt.Errorf("DELETE operations on %q are not supported", vt.name)
}
