.PHONY: up down logs topics test install clean

## Start the stack (detached)
up:
	docker compose up -d
	@echo "Waiting for Kafka to become healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null | grep -q healthy; do \
		printf '.'; sleep 2; \
	done
	@echo " Kafka is healthy."

## Create the standard topic set
topics: up
	./scripts/create_topics.sh

## Stop and remove containers (volumes preserved)
down:
	docker compose down

## Stop and wipe all volumes (full reset)
clean:
	docker compose down -v

## Tail logs
logs:
	docker compose logs -f

## Install Python test dependencies
install:
	pip3 install -r requirements.txt

## Run all integration tests (stack must be up)
test:
	python3 -m pytest tests/ -v

## Run a specific test file, e.g.:  make test-file f=tests/test_schema_registry.py
test-file:
	python3 -m pytest $(f) -v

## Run tests matching a specific name pattern, e.g.:  make test-filter k=avro
test-filter:
	python3 -m pytest tests/ -v -k "$(k)"
