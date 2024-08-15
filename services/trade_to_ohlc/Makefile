build:
	docker build -t trade-to-ohlc .

run-dev: 
	KAFKA_BROKER_ADDRESS='localhost:19092' \
	OHLC_WINDOW_SECONDS=10 \
	poetry run python -m src.main

run-debug: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e OHLC_WINDOW_SECONDS=60 \
		trade-to-ohlc

run: build
	docker run \
		-d \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e OHLC_WINDOW_SECONDS=60 \
		trade-to-ohlc

lint:
	ruff check --fix

format:
	ruff format .
