build:
	docker build -t trade-producer .

run-dev: 
	poetry run python -m src.main

run-debug: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e LIVE_OR_HISTORICAL=live \
		trade-producer

run: build
	docker run \
		-d \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e LIVE_OR_HISTORICAL=live \
		trade-producer

lint:
	ruff check --fix

format:
	ruff format .
