package extension

import (
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/nats/config"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule(config.DefaultPublisherVTabName, &PublisherModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateModule(config.DefaultSubscriberVTabName, &SubscriberModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateModule(config.DefaultJetStreamPublisherVTabName, &JetStreamPublisherModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateModule(config.DefaultJetStreamSubscriberVTabName, &JetStreamSubscriberModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("nats_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}
