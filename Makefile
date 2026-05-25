.PHONY: build up down logs reset-db clean

build:
	docker compose build

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

logs-cp:
	docker compose logs -f control-plane

logs-workers:
	docker compose logs -f worker-1 worker-2 worker-3

reset-db:
	docker compose down -v
	docker compose up --build -d

clean:
	docker compose down -v --rmi local

chaos-kill-worker:
	@WORKER=$$(docker compose ps --format '{{.Name}}' | grep worker | shuf -n 1); \
	echo "Killing $$WORKER..."; \
	docker kill $$WORKER
