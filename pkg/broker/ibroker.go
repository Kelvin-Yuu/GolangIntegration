package broker

type Broker interface {
	Options() *Options
	NewProducer() (Producer, error)
	NewConsumer(opts ...SubscribeOption) (Consumer, error)
	Close() error
	Type() string
}

type Producer interface {
	Publish(topic string, m *Message) error
}

type Consumer interface {
	GroupID() string
	Topic() string
	Subscribe(topic string, h Handler) error
	Unsubscribe() error
}

type Message struct {
	Header map[string]string
	Body   []byte
}

type Event interface {
	Topic() string
	Message() *Message
	Error() error
	Ack() error
}

type Handler func(Event) error
