#!/bin/bash
while [ true ]
do
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka < /tmp/10.json
sleep 10
done
