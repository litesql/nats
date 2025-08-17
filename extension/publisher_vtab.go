package extension

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/walterwanderley/sqlite"
)

type PublisherVirtualTable struct {
	nc           *nats.Conn
	name         string
	logger       *slog.Logger
	loggerCloser io.Closer
}

func NewPublisherVirtualTable(name string, servers string, opts []nats.Option, loggerDef string) (*PublisherVirtualTable, error) {
	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}

	var nc *nats.Conn
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
		nc, err = nats.Connect(servers, opts...)
		if err != nil {
			if loggerCloser != nil {
				loggerCloser.Close()
			}
			return nil, fmt.Errorf("failed to connect to nats: %w", err)
		}
	}

	return &PublisherVirtualTable{
		name:         name,
		nc:           nc,
		logger:       logger,
		loggerCloser: loggerCloser,
	}, nil
}

func (vt *PublisherVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{}, nil
}

func (vt *PublisherVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return nil, fmt.Errorf("SELECT operations on %q is not supported", vt.name)
}

func (vt *PublisherVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	if vt.nc != nil {
		vt.nc.Close()
	}
	return err
}

func (vt *PublisherVirtualTable) Destroy() error {
	return nil
}

func (vt *PublisherVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	subject := values[0].Text()
	if subject == "" {
		return 0, fmt.Errorf("subject is required")
	}
	data := values[1].Text()
	err := vt.nc.Publish(subject, []byte(data))
	if err != nil {
		return 0, fmt.Errorf("failed to publish message: %w", err)
	}
	return 1, nil
}

func (vt *PublisherVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.name)
}

func (vt *PublisherVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q is not supported", vt.name)
}

func (vt *PublisherVirtualTable) Delete(_ sqlite.Value) error {
	return fmt.Errorf("DELETE operations on %q is not supported", vt.name)
}
