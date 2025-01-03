## Kafka Work 1

Предварительные требования:
- установлен golang не ниже версии 1.22
- установлен docker (docker-compose)
- настройка kafka

Последовательность действий для запуска:
1. склонировать репозитарий локально
2. выполнить `sudo docker-compose up`
3. зайти в терминал контейнера с брокером и выполнить:
   1. `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic msg_topic   --partitions 3 --replication-factor 2`
   2. `kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name msg_topic --add-config min.insync.replicas=2`
4. выполнить в корне проекта `go mod tidy`

Запуск:
- перейти в директорию проекта
- для запуска продьюсера make p (или `go run cmd/producer/main.go <bootstrap> <topic>`)
- для запуска консьюмера pull: make conspull (или `go run cmd/consumer_pull/main.go <bootstrap> <group> <topic>`)
- для запуска консьюмера push: make conspush (или `go run cmd/consumer_push/main.go <bootstrap> <group> <topic>`)


Для просмотра сообщений можно открыть web ui: `http://localhost:8080/ui/

для получение информации о топике нужно выполнить в контейнере брокера: `kafka-topics.sh --describe --topic my-topic --bootstrap-server localhost:9092`