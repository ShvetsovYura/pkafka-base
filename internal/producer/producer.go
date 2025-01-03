package producer

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/ShvetsovYura/pkafka_base/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const createMessageInterval time.Duration = 1 * time.Second

type Msg struct {
	Uuid  string `json:"uuid"`
	Value string `json:"value"`
}

type KProducer struct {
	topic     string
	servers   string
	producer  *kafka.Producer
	partition kafka.TopicPartition
}

func NewKafkaProducer(topic string, servers string) *KProducer {
	logger.Init()
	// Создание продюсера
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        servers,
		"acks":                     "all",
		"message.send.max.retries": 3,
	})
	if err != nil {
		log.Fatalf("Невозможно создать продюсера: %s\n", err)
	}
	partition := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
	logger.Log.Info("Продюсер создан", slog.Any("producer", p))

	return &KProducer{
		topic:     topic,
		servers:   servers,
		producer:  p,
		partition: partition,
	}

}

func (p *KProducer) Run(ctx context.Context, wg *sync.WaitGroup) {
	deliveryChan := make(chan kafka.Event)

	defer func() {
		p.producer.Close()
		close(deliveryChan)
	}()

	// Канал доставки событий (информации об отправленном сообщении)
	timer := time.NewTicker(createMessageInterval)
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Получен сигнал выхода, остановка продьюсера...")
			wg.Done()
		case <-timer.C:
			msg := &Msg{
				Uuid:  uuid.NewString(),
				Value: "value",
			}

			payload, err := json.Marshal(msg)
			if err != nil {
				log.Fatalf("Невозможно сериализовать сообщение: %s\n", err)
			}

			// Отправляем сообщение в брокер
			err = p.producer.Produce(&kafka.Message{
				TopicPartition: p.partition,
				Value:          payload,
			}, deliveryChan)
			if err != nil {
				log.Fatalf("Ошибка при отправке сообщения: %v\n", err)
			}
			e := <-deliveryChan

			// Приводим Events к типу *kafka.Message
			m := e.(*kafka.Message)

			// Если возникла ошибка доставки сообщения
			if m.TopicPartition.Error != nil {
				logger.Log.Error("Ошибка доставки сообщения", slog.Any("error", m.TopicPartition.Error))
			} else {
				logger.Log.Info("Sent",
					slog.String("topic", *m.TopicPartition.Topic),
					slog.Any("partition", m.TopicPartition.Partition),
					slog.String("offset", m.TopicPartition.Offset.String()),
					slog.String("message", string(payload)))
			}

		}
	}

}
