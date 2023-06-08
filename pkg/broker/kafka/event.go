package kafka

import (
	"GolangIntegration/pkg/broker"
	"GolangIntegration/pkg/logger"
	"github.com/Shopify/sarama"
)

type Event struct {
	topic   string
	message *broker.Message
	err     error

	cm   *sarama.ConsumerMessage
	sess sarama.ConsumerGroupSession
}

func (evt *Event) Topic() string {
	return evt.topic
}

func (evt *Event) Message() *broker.Message {
	return evt.message
}

func (evt *Event) Error() error {
	return evt.err
}

func (evt *Event) Ack() error {
	evt.sess.MarkMessage(evt.cm, "")
	return nil
}

type consumerGroupHandler struct {
	opts broker.SubscribeOptions
	cg   sarama.ConsumerGroup
	sess sarama.ConsumerGroupSession

	handler      broker.Handler
	errorHandler broker.Handler
}

func (c *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		headers := make(map[string]string)
		for _, v := range msg.Headers {
			headers[string(v.Key)] = string(v.Value)
		}
		event := &Event{
			topic: msg.Topic,
			message: &broker.Message{
				Header: headers,
				Body:   msg.Value,
			},
			cm:   msg,
			sess: session,
		}

		err := c.handler(event)
		if err == nil && c.opts.AutoAck {
			session.MarkMessage(msg, "")
		} else if err != nil {
			event.err = err
			if c.errorHandler != nil {
				err := c.errorHandler(event)
				if err != nil {
					return err
				}
			}
			logger.ErrorF("[kafka]: subscriber errors: %v", err)
		}
	}
	return nil
}
