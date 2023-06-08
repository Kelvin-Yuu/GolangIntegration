package kafka

import (
	"GolangIntegration/pkg/broker"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"sync"
)

type kBroker struct {
	opts *broker.Options

	pr  *Producer
	crs []*Consumer

	sc      []sarama.Client
	scMutex sync.RWMutex
}

func (k *kBroker) Options() *broker.Options {
	return k.opts
}

func (k *kBroker) NewProducer() (broker.Producer, error) {
	k.pr = &Producer{
		opts:      k.opts,
		connected: false,
	}
	return k.pr, k.pr.Connect()
}

func (k *kBroker) NewConsumer(opts ...broker.SubscribeOption) (broker.Consumer, error) {
	opt := broker.SubscribeOptions{
		AutoAck: true,
		GroupID: uuid.New().String(),
	}
	for _, o := range opts {
		o(&opt)
	}

	addr := k.opts.Addr
	if len(addr) == 0 {
		addr = []string{"127.0.0.1:9092"}
	}
	config := k.opts.Kafka
	if config == nil {
		config = sarama.NewConfig()
	}
	if !config.Version.IsAtLeast(sarama.V3_3_1_0) {
		config.Version = sarama.V3_3_1_0
	}
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	cs, err := sarama.NewClient(addr, config)
	if err != nil {
		return nil, err
	}
	k.scMutex.Lock()
	defer k.scMutex.Unlock()
	k.sc = append(k.sc, cs)

	cg, err := sarama.NewConsumerGroupFromClient(opt.GroupID, cs)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		opts:         opt,
		cg:           cg,
		errorHandler: k.opts.ErrorHandler,
	}
	k.crs = append(k.crs, consumer)

	return consumer, nil
}

func (k *kBroker) Close() error {
	k.scMutex.Lock()
	defer k.scMutex.Unlock()

	for _, cr := range k.crs {
		err := cr.Unsubscribe()
		if err != nil {
			return err
		}
	}

	for _, client := range k.sc {
		err := client.Close()
		if err != nil {
			return err
		}
	}
	k.sc = nil
	return k.pr.Disconnect()
}

func (k *kBroker) Type() string {
	return "kafka"
}

func NewBroker(opts *broker.Options) broker.Broker {
	return &kBroker{
		opts: opts,
	}
}
