package extension

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/walterwanderley/sqlite"
)

type JetStreamSubscriberVirtualTable struct {
	virtualTableName string
	tableName        string
	js               jetstream.JetStream
	timeout          time.Duration
	consumers        []consumer
	stmt             *sqlite.Stmt
	stmtMu           sync.Mutex
	mu               sync.Mutex
	logger           *slog.Logger
	loggerCloser     io.Closer
}

type consumer struct {
	cc      jetstream.ConsumeContext
	stream  string
	durable string
	subject string
}

func NewJetStreamSubscriberVirtualTable(virtualTableName string, servers string, opts []nats.Option, timeout time.Duration, tableName string, conn *sqlite.Conn, loggerDef string) (*JetStreamSubscriberVirtualTable, error) {

	stmt, _, err := conn.Prepare(fmt.Sprintf(`INSERT INTO %s(subject, data, headers, reply, timestamp) VALUES(?, ?, ?, ?, ?)`, tableName))
	if err != nil {
		return nil, err
	}

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

	return &JetStreamSubscriberVirtualTable{
		virtualTableName: virtualTableName,
		tableName:        tableName,
		js:               js,
		timeout:          timeout,
		logger:           logger,
		loggerCloser:     loggerCloser,
		consumers:        make([]consumer, 0),
		stmt:             stmt,
	}, nil
}

func (vt *JetStreamSubscriberVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *JetStreamSubscriberVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newJetStreamSubscriptionsCursor(vt.consumers), nil
}

func (vt *JetStreamSubscriberVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	for _, consumer := range vt.consumers {
		consumer.cc.Stop()
	}
	if vt.js != nil {
		vt.js.Conn().Close()
	}

	return errors.Join(err, vt.stmt.Finalize())
}

func (vt *JetStreamSubscriberVirtualTable) Destroy() error {
	return nil
}

func (vt *JetStreamSubscriberVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	if vt.js == nil {
		return 0, fmt.Errorf("not connected to jetstream")
	}
	stream := values[0].Text()
	if stream == "" {
		return 0, fmt.Errorf("stream is invalid")
	}
	subject := values[1].Text()
	durable := values[2].Text()

	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(stream) {
		return 0, fmt.Errorf("already subscribed to the %q stream", stream)
	}
	var (
		ctx    = context.Background()
		cancel func()
	)
	if vt.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, vt.timeout)
		defer cancel()
	}
	c, err := vt.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		Durable:       durable,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to subscribe: %w", err)
	}
	cc, err := c.Consume(vt.messageHandler)
	if err != nil {
		return 0, fmt.Errorf("failed to consume: %w", err)
	}
	vt.consumers = append(vt.consumers, consumer{
		cc:      cc,
		stream:  stream,
		subject: subject,
		durable: durable,
	})
	return 1, nil
}

func (vt *JetStreamSubscriberVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.virtualTableName)
}

func (vt *JetStreamSubscriberVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *JetStreamSubscriberVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--

	if index >= 0 && index < len(vt.consumers) {
		subscription := vt.consumers[index]
		subscription.cc.Stop()
		vt.consumers = slices.Delete(vt.consumers, index, index+1)
	}
	return nil
}

func (vt *JetStreamSubscriberVirtualTable) contains(stream string) bool {
	for _, consumer := range vt.consumers {
		if consumer.stream == stream {
			return true
		}
	}
	return false
}

func (vt *JetStreamSubscriberVirtualTable) messageHandler(msg jetstream.Msg) {
	vt.stmtMu.Lock()
	defer vt.stmtMu.Unlock()
	err := vt.stmt.Reset()
	if err != nil {
		vt.logger.Error("reset statement", "error", err, "subject", msg.Subject(), "reply", msg.Reply())
		return
	}
	var headers bytes.Buffer
	json.NewEncoder(&headers).Encode(msg.Headers())

	vt.stmt.BindText(1, msg.Subject())
	vt.stmt.BindText(2, string(msg.Data()))
	vt.stmt.BindText(3, headers.String())
	vt.stmt.BindText(4, msg.Reply())
	vt.stmt.BindText(5, time.Now().Format(time.RFC3339Nano))
	_, err = vt.stmt.Step()
	if err != nil {
		vt.logger.Error("insert data", "error", err, "subject", msg.Subject(), "reply", msg.Reply())
		return
	}
	msg.Ack()
}

type jetstreamSubscriptionsCursor struct {
	data    []consumer
	current consumer // current row that the cursor points to
	rowid   int64    // current rowid .. negative for EOF
}

func newJetStreamSubscriptionsCursor(data []consumer) *jetstreamSubscriptionsCursor {
	slices.SortFunc(data, func(a, b consumer) int {
		return cmp.Compare(a.stream, b.stream)
	})
	return &jetstreamSubscriptionsCursor{
		data: data,
	}
}

func (c *jetstreamSubscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *jetstreamSubscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText(c.current.stream)
	case 1:
		ctx.ResultText(c.current.subject)
	case 2:
		ctx.ResultText(c.current.durable)
	}

	return nil
}

func (c *jetstreamSubscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *jetstreamSubscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *jetstreamSubscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *jetstreamSubscriptionsCursor) Close() error {
	return nil
}
