start-redpanda:
	docker compose -f redpanda.yaml up -d

stop-redpanda:
	docker compose -f redpanda.yaml down

start-feature-pipeline:
	docker compose -f feature-pipeline.yaml up --build -d

stop-feature-pipeline:
	docker compose -f feature-pipeline.yaml down