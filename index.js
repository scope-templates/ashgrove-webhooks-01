const express = require('express');
const IORedis = require('ioredis');
const { Queue } = require('bullmq');

const PORT = Number(process.env.PORT) || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

// enableOfflineQueue: false makes the enqueue route fail fast while Redis is unreachable
// instead of buffering commands until it comes back.
const connection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null, enableOfflineQueue: false });
let redisError = null;
connection.on('error', (err) => {
  redisError = err.message;
});
connection.on('ready', () => {
  redisError = null;
});

const queue = new Queue('orders', {
  connection,
  defaultJobOptions: { attempts: 3, backoff: { type: 'exponential', delay: 1000 }, removeOnComplete: 1000, removeOnFail: 5000 },
});
queue.on('error', (err) => {
  redisError = err.message;
});

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.post('/webhooks/orders', async (req, res) => {
  const { id, total, currency } = req.body ?? {};
  const problems = [];
  if (typeof id !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(id)) {
    problems.push('id must be a string of 1 to 64 letters, digits, "_" or "-"');
  }
  if (typeof total !== 'number' || !Number.isFinite(total) || total < 0) {
    problems.push('total must be a non-negative number');
  }
  if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) {
    problems.push('currency must be a three-letter ISO 4217 code');
  }
  if (problems.length > 0) {
    return res.status(400).json({ error: 'invalid order', problems });
  }

  if (connection.status !== 'ready') {
    return res.status(503).json({ error: 'queue unavailable', detail: redisError ?? `redis connection is ${connection.status}` });
  }
  try {
    await queue.add('order', { id, total, currency, receivedAt: new Date().toISOString() }, { jobId: id });
  } catch (err) {
    return res.status(503).json({ error: 'queue unavailable', detail: err.message });
  }
  res.status(202).json({ queued: id });
});

app.use((err, req, res, next) => {
  res.status(err.status ?? 500).json({ error: err.message });
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`ashgrove-webhooks listening on 0.0.0.0:${PORT}`);
});

function shutdown() {
  server.close(() => process.exit(0));
  connection.disconnect();
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
