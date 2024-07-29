build:
	docker build -t trade-producer .

run: build
	docker run -it --rm --network=redpanda_network trade-producer

lint:
	ruff check --fix

format:
	ruff format .
