package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Msg struct {
	Uuid  string `json:"uuid"`
	Value string `json:"value"`
}

type KConsumer struct {
	topics           []string
	group            string
	servers          string
	timeout          time.Duration
	consumer         *kafka.Consumer
	enableAutoCommit bool
}

func NewKafkaConsumer(topics []string, group string, servers string, poolTimeout time.Duration, autoOffsetReset string, enableAutoCommit bool) *KConsumer {

	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  servers,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"enable.auto.commit": enableAutoCommit,
		"auto.offset.reset":  autoOffsetReset,
		"fetch.min.bytes":    1024 * 10,
		// "max.poll.records":   1, // это не работатет https://github.com/confluentinc/confluent-kafka-go/issues/380)
	})

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
	}

	fmt.Printf("Консьюмер создан %v\n", c)

	return &KConsumer{
		topics:           topics,
		group:            group,
		servers:          servers,
		timeout:          poolTimeout,
		enableAutoCommit: enableAutoCommit,
		consumer:         c,
	}
}

func (c *KConsumer) Run(ctx context.Context) {
	defer c.consumer.Close()
	err := c.consumer.SubscribeTopics(c.topics, nil)
	var timer *time.Ticker

	// если отключен автокаммит,
	// то значит это pull consumer (интарвальное получение сообщений)
	// в противном случае - это push consumer (получение сообщений сразу)
	if !c.enableAutoCommit {
		timer = time.NewTicker(c.timeout)
	}

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	run := true
	for run {
		select {
		case <-ctx.Done():
			fmt.Printf("Передан сигнал завершения приложения приложение останавливается\n")
			run = false
		default:
			// не знаю как сделать по-другому чтение сообщений
			// через определенный интервал времени
			// .Poll() с таймаутом не принес ожидаемого результата
			if timer != nil {
				<-timer.C
			}
			c.messageProccess()
		}
	}
}

func (c *KConsumer) messageProccess() {
	msg, err := c.consumer.ReadMessage(c.timeout)
	if err == nil {
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		// в зависимости от настройки
		if !c.enableAutoCommit {
			_, err = c.consumer.CommitMessage(msg)
			if err != nil {
				fmt.Printf("error on commit message %v\n", msg)
			}
		}
	} else if !err.(kafka.Error).IsTimeout() {
		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	}
}
