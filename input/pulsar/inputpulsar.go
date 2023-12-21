package pulsar

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
)

const ModuleName = "pulsar"

// defaults
const (
	DefaultMaxConnectionsPerBroker = 1
	DefaultConnectionTimeout       = time.Second * 10
	DefaultNackRedeliveryDelay     = 10 * time.Second
	DefaultReceiverQueueSize       = 20
)

type InputConfig struct {
	config.InputConfig
	Name                    string        `json:"name"`
	ServiceURL              string        `json:"service_url"`
	SubscriptionName        string        `json:"subscription_name"`
	Topics                  []string      `json:"topics"`
	SubscriptionType        string        `json:"subscription_type"`
	MaxConnectionsPerBroker int           `json:"max_connections_per_broker"`
	ConnectionTimeout       time.Duration `toml:"connection_timeout"`
	NackRedeLiveryDelay     time.Duration `toml:"nack_redelivery_delay"`
	ReceiverQueueSize       int           `toml:"receiver_queue_size"`
}

func DefaultInputConfig() InputConfig {
	return InputConfig{
		InputConfig: config.InputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		MaxConnectionsPerBroker: DefaultMaxConnectionsPerBroker,
		ConnectionTimeout:       DefaultConnectionTimeout,
		NackRedeLiveryDelay:     DefaultNackRedeliveryDelay,
		ReceiverQueueSize:       DefaultReceiverQueueSize,
	}
}

func InitHandler(
	ctx context.Context,
	raw config.ConfigRaw,
	control config.Control,
) (config.TypeInputConfig, error) {
	conf := DefaultInputConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}

	conf.Codec, err = config.GetCodecOrDefault(ctx, raw["codec"])
	return nil, err
}

func (t *InputConfig) Start(ctx context.Context, msgChan chan<- logevent.LogEvent) (err error) {
	client, err := pulsar.NewClient(
		pulsar.ClientOptions{
			URL:                     t.ServiceURL,
			ConnectionTimeout:       t.ConnectionTimeout,
			MaxConnectionsPerBroker: t.MaxConnectionsPerBroker,
		})
	if err != nil {
		goglog.Logger.Errorf("Error from creation of pulsar client: %v", err)
		return err
	}

	options := pulsar.ConsumerOptions{
		Type: parseSubscriptionType(t.SubscriptionType), SubscriptionName: t.SubscriptionName,
		Topics:                      t.Topics,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        t.Name,
		NackRedeliveryDelay:         t.NackRedeLiveryDelay,
		ReceiverQueueSize:           t.ReceiverQueueSize,
	}
	consumer, err := client.Subscribe(options)
	if err != nil {
		goglog.Logger.Errorf("Error from subscription of pulsar topics: %v", err)
		return err
	}

	ct, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			msg, err := consumer.Receive(ct)
			if err != nil {
				goglog.Logger.Errorf("Error from consumer: %v", err)
				continue
			}

			var extra = map[string]any{
				"topic":     msg.Topic,
				"timestamp": msg.EventTime(),
			}
			ok, err := t.Codec.Decode(ct, string(msg.Payload()), extra, []string{}, msgChan)
			if !ok {
				goglog.Logger.Errorf("decode message to msg chan error : %v", err)
			}

			err = consumer.Ack(msg)
			if err != nil {
				goglog.Logger.Errorf("acknowledge message to pulsar error : %v", err)
			}

			// check if context was canceled, signaling that the consumer should stop
			if ct.Err() != nil {
				return
			}
		}
	}()

	goglog.Logger.Println("pulsar consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ct.Done():
		goglog.Logger.Println("terminating: context canceled")
	case <-sigterm:
		goglog.Logger.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	client.Close()
	consumer.Close()
	return nil
}

func parseSubscriptionType(st string) pulsar.SubscriptionType {
	switch st {
	case "Exclusive":
		return pulsar.Exclusive
	case "Shared":
		return pulsar.Shared
	case "FailOver":
		return pulsar.Failover
	case "KeyShared":
		return pulsar.KeyShared
	}
	return pulsar.Exclusive
}
