build:
	docker build -t kafka-to-feature-store .

run-dev: 
	poetry run python -m src.main

run-dev-historical: 
	LIVE_OR_HISTORICAL=historical poetry run python -m src.main

run: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e LIVE_OR_HISTORICAL=live \
		kafka-to-feature-store

run-historical: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e LIVE_OR_HISTORICAL=historical \
		kafka-to-feature-store

lint:
	ruff check --fix

format:
	ruff format .
