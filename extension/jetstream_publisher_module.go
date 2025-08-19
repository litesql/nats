package extension

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/nats/config"
)

type JetStreamPublisherModule struct {
}

func (m *JetStreamPublisherModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	virtualTableName := args[2]
	if virtualTableName == "" {
		virtualTableName = config.DefaultJetStreamPublisherVTabName
	}

	var (
		servers string
		opts    = make([]nats.Option, 0)

		timeout  time.Duration
		username string
		password string

		certFilePath    string
		certKeyFilePath string
		caFilePath      string
		insecure        bool

		err    error
		logger string
	)
	if len(args) > 3 {
		for _, opt := range args[3:] {
			k, v, ok := strings.Cut(opt, "=")
			if !ok {
				return nil, fmt.Errorf("invalid option: %q", opt)
			}
			k = strings.TrimSpace(k)
			v = sanitizeOptionValue(v)

			switch strings.ToLower(k) {
			case config.ClientName:
				opts = append(opts, nats.Name(v))
			case config.Timeout:
				i, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %w", k, err)
				}
				timeout = time.Duration(i) * time.Millisecond
				opts = append(opts, nats.Timeout(timeout))
			case config.Servers:
				servers = v
			case config.Username:
				username = v
			case config.Password:
				password = v
			case config.Token:
				opts = append(opts, nats.Token(v))
			case config.CertFile:
				certFilePath = v
			case config.CertKeyFile:
				certKeyFilePath = v
			case config.CertCAFile:
				caFilePath = v
			case config.Insecure:
				insecure, err = strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %v", k, err)
				}
			case config.Logger:
				logger = v
			}
		}
	}

	tlsConfig := tls.Config{
		InsecureSkipVerify: insecure,
	}

	if certFilePath != "" && certKeyFilePath != "" {
		clientCert, err := tls.LoadX509KeyPair(certFilePath, certKeyFilePath)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	if caFilePath != "" {
		caCertPEM, err := os.ReadFile(caFilePath)
		if err != nil {
			return nil, fmt.Errorf("error loading CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCertPEM) {
			return nil, fmt.Errorf("error appending CA certificate to pool")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if strings.HasPrefix(servers, "tls://") {
		opts = append(opts, nats.Secure(&tlsConfig))
	}

	if username != "" {
		opts = append(opts, nats.UserInfo(username, password))
	}

	vtab, err := NewJetStreamPublisherVirtualTable(virtualTableName, servers, opts, timeout, logger)
	if err != nil {
		return nil, err
	}

	return vtab,
		declare("CREATE TABLE x(subject TEXT, data BLOB)")
}
