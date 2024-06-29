#!/bin/bash

docker compose up -d

docker exec -it broker kafka-topics --create --topic=src-java-wikimedia-recentchange --bootstrap-server=localhost:9092 --partitions=3