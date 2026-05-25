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

chaos-kill-control-plane:
	@echo "Killing control-plane..."
	docker compose kill control-plane
	@echo "Restarting control-plane..."
	docker compose up -d control-plane
	@echo "Control plane restarted. Workers will auto-reconnect."

chaos-kill-worker:
	@SERVICE=$$(docker compose ps --services | grep worker | shuf -n 1); \
	echo "Restarting $$SERVICE..."; \
	docker compose restart $$SERVICE

chaos-duplicate:
	@echo "Fetching most recent dispatch..."; \
	TC_ID=$$(curl -s http://localhost:8000/debug/dispatches | python3 -c "import sys,json; d=json.load(sys.stdin)['dispatches']; print(d[0]['tool_call_id'] if d else '')"); \
	if [ -z "$$TC_ID" ]; then echo "No dispatches found"; exit 1; fi; \
	echo "Replaying tool_call_id=$$TC_ID..."; \
	curl -s -X POST "http://localhost:8000/debug/replay/$$TC_ID" | python3 -m json.tool
