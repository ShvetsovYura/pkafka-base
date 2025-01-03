conspull: 
	go run cmd/consumer_pull/main.go localhost:9094 group1 msg_topic
conspush: 
	go run cmd/consumer_push/main.go localhost:9094 group2 msg_topic
p:
	go run cmd/producer/main.go localhost:9094 mgs_topic
