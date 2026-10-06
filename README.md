# Ashgrove Webhooks

Ashgrove Webhooks, an order webhook receiver with a background queue. The web process
validates incoming order webhooks and puts them on a queue named `orders`; the worker
process takes them off the queue and processes them.

## Run locally

Needs a Redis server (default `redis://127.0.0.1:6379`).

    npm install
    npm start          # web: GET /health, POST /webhooks/orders
    npm run worker     # worker: consumes the "orders" queue

Send a test order:

    curl -s -X POST localhost:3000/webhooks/orders -H 'content-type: application/json' \
      -d '{"id":"ord_1001","total":42.5,"currency":"EUR"}'

## Routes

- `GET /health` returns `{"ok": true}`
- `POST /webhooks/orders` JSON body `{"id": "ord_1001", "total": 42.5, "currency": "EUR"}`;
  answers `202 {"queued": "ord_1001"}`, `400` with the list of problems for an invalid body,
  `503` while Redis is unreachable

The web process starts and answers `/health` even when Redis is down. The worker gives up
after ten connection attempts and exits with a message.

## Environment

- `PORT` web port (default 3000)
- `REDIS_URL` Redis connection string (default `redis://127.0.0.1:6379`)
