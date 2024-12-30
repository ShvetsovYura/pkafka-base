package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

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
	log.Printf("Продюсер создан %v\n", p)

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
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Получен сигнал выхода, остановка продьюсера...")
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
				fmt.Printf("Ошибка доставки сообщения: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Sent to topic %s [%d] offset %v message: %s\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, payload)
			}

		}
	}

}
