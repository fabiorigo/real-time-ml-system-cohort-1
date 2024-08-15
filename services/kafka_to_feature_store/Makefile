build:
	docker build -t kafka-to-feature-store .

run-dev: 
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	FEATURE_GROUP_SEND_SECONDS=30 \
	poetry run python -m src.main

run-debug: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e FEATURE_GROUP_SEND_SECONDS=30 \
		kafka-to-feature-store

run: build
	docker run \
		-d \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e FEATURE_GROUP_SEND_SECONDS=30 \
		kafka-to-feature-store

lint:
	ruff check --fix

format:
	ruff format .
