package kafka

import (
	"GolangIntegration/pkg/broker"
	"GolangIntegration/pkg/logger"
	"context"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	opts broker.SubscribeOptions

	cg    sarama.ConsumerGroup
	topic string

	errorHandler broker.Handler
}

func (cr *Consumer) GroupID() string {
	return cr.opts.GroupID
}

func (cr *Consumer) Topic() string {
	return cr.topic
}

func (cr *Consumer) Subscribe(topic string, handler broker.Handler) error {
	cr.topic = topic
	h := &consumerGroupHandler{
		opts:         cr.opts,
		cg:           cr.cg,
		handler:      handler,
		errorHandler: cr.errorHandler,
	}

	ctx := context.Background()
	topics := []string{topic}

	go func() {
		for {
			select {
			case err := <-cr.cg.Errors():
				if err != nil {
					if cr.errorHandler != nil {
						msg := &Event{
							topic: topic,
							err:   err,
						}
						err := cr.errorHandler(msg)
						if err != nil {
							return
						}
					}
					logger.ErrorF("k_consumer errors:", err)
				}
			default:
				err := cr.cg.Consume(ctx, topics, h)
				switch err {
				case sarama.ErrClosedConsumerGroup:
					return
				case nil:
					continue
				default:
					if cr.errorHandler != nil {
						msg := &Event{
							topic: topic,
							err:   err,
						}
						err := cr.errorHandler(msg)
						if err != nil {
							return
						}
					}

					logger.Error(err)
				}

			}
		}
	}()
	return nil
}

func (cr *Consumer) Unsubscribe() error {
	return cr.cg.Close()
}
