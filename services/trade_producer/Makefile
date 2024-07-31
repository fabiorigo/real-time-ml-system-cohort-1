build:
	docker build -t trade-producer .

run-dev: 
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	poetry run python src/main.py

run-debug: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		trade-producer

run: build
	docker run \
		-d \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		trade-producer

lint:
	ruff check --fix

format:
	ruff format .
