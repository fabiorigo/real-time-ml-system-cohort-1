build:
	docker build -t trade-producer .

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
		trade-producer

run-historical: build
	docker run \
		-it \
		--rm \
		--network=redpanda_network \
		-e KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		-e LIVE_OR_HISTORICAL=historical \
		trade-producer

lint:
	ruff check --fix

format:
	ruff format .
