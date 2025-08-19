package extension

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/walterwanderley/sqlite"
)

type SubscriberVirtualTable struct {
	virtualTableName string
	tableName        string
	nc               *nats.Conn
	subs             []*nats.Subscription
	stmt             *sqlite.Stmt
	stmtMu           sync.Mutex
	mu               sync.Mutex
	logger           *slog.Logger
	loggerCloser     io.Closer
}

func NewSubscriberVirtualTable(virtualTableName string, servers string, opts []nats.Option, tableName string, conn *sqlite.Conn, loggerDef string) (*SubscriberVirtualTable, error) {

	stmt, _, err := conn.Prepare(fmt.Sprintf(`INSERT INTO %s(subject, data, timestamp) VALUES(?, ?, ?)`, tableName))
	if err != nil {
		return nil, err
	}

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

	return &SubscriberVirtualTable{
		virtualTableName: virtualTableName,
		tableName:        tableName,
		nc:               nc,
		logger:           logger,
		loggerCloser:     loggerCloser,
		subs:             make([]*nats.Subscription, 0),
		stmt:             stmt,
	}, nil
}

func (vt *SubscriberVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *SubscriberVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newSubscriptionsCursor(vt.subs), nil
}

func (vt *SubscriberVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	for _, sub := range vt.subs {
		err = errors.Join(err, sub.Unsubscribe())
	}
	if vt.nc != nil {
		vt.nc.Close()
	}

	return errors.Join(err, vt.stmt.Finalize())
}

func (vt *SubscriberVirtualTable) Destroy() error {
	return nil
}

func (vt *SubscriberVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	if vt.nc == nil {
		return 0, fmt.Errorf("not connected to nats")
	}
	subject := values[0].Text()
	if subject == "" {
		return 0, fmt.Errorf("subject is invalid")
	}
	queue := values[1].Text()

	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(subject) {
		return 0, fmt.Errorf("already subscribed to the %q subject", subject)
	}
	subscription, err := vt.nc.QueueSubscribe(subject, queue, vt.messageHandler)
	if err != nil {
		return 0, fmt.Errorf("failed to subscribe: %w", err)
	}
	vt.subs = append(vt.subs, subscription)
	return 1, nil
}

func (vt *SubscriberVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.virtualTableName)
}

func (vt *SubscriberVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *SubscriberVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--

	if index >= 0 && index < len(vt.subs) {
		subscription := vt.subs[index]
		err := subscription.Unsubscribe()
		if err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
		vt.subs = slices.Delete(vt.subs, index, index+1)
	}
	return nil
}

func (vt *SubscriberVirtualTable) contains(subject string) bool {
	for _, subscription := range vt.subs {
		if subscription.Subject == subject {
			return true
		}
	}
	return false
}

func (vt *SubscriberVirtualTable) messageHandler(msg *nats.Msg) {
	vt.stmtMu.Lock()
	defer vt.stmtMu.Unlock()
	err := vt.stmt.Reset()
	if err != nil {
		vt.logger.Error("reset statement", "error", err, "subject", msg.Subject, "queue", msg.Sub.Queue)
		return
	}

	vt.stmt.BindText(1, msg.Subject)
	vt.stmt.BindText(2, string(msg.Data))
	vt.stmt.BindText(3, time.Now().Format(time.RFC3339Nano))
	_, err = vt.stmt.Step()
	if err != nil {
		vt.logger.Error("insert data", "error", err, "subject", msg.Subject, "queue", msg.Sub.Queue)
		return
	}
	msg.Ack()
}

type subscriptionsCursor struct {
	data    []*nats.Subscription
	current *nats.Subscription // current row that the cursor points to
	rowid   int64              // current rowid .. negative for EOF
}

func newSubscriptionsCursor(data []*nats.Subscription) *subscriptionsCursor {
	slices.SortFunc(data, func(a, b *nats.Subscription) int {
		return cmp.Compare(a.Subject, b.Subject)
	})
	return &subscriptionsCursor{
		data: data,
	}
}

func (c *subscriptionsCursor) Next() error {
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

func (c *subscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	if c.current != nil {
		switch i {
		case 0:
			ctx.ResultText(c.current.Subject)
		case 1:
			ctx.ResultText(c.current.Queue)
		}
	}
	return nil
}

func (c *subscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *subscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *subscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *subscriptionsCursor) Close() error {
	return nil
}
