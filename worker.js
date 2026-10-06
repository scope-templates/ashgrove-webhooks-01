const IORedis = require('ioredis');
const { Worker } = require('bullmq');

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

let closing = false;
let lastError = null;
const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
  retryStrategy: (attempt) => (attempt > 10 ? null : Math.min(attempt * 200, 2000)),
});
connection.on('error', (err) => {
  lastError = err.message;
});
connection.on('end', () => {
  if (closing) return;
  console.error(`could not connect to Redis after 10 attempts (${lastError}); exiting`);
  process.exit(1);
});

const worker = new Worker(
  'orders',
  async (job) => {
    const { id, total, currency } = job.data;
    console.log(`processing order ${id}: ${total} ${currency}`);
    await new Promise((resolve) => setTimeout(resolve, 200));
    return { processedAt: new Date().toISOString() };
  },
  { connection, concurrency: 5 },
);

worker.on('completed', (job) => {
  console.log(`order ${job.data.id} processed`);
});
worker.on('failed', (job, err) => {
  console.error(`order ${job?.data?.id ?? job?.id} failed: ${err.message}`);
});
// BullMQ keeps a second, blocking connection, so one outage is reported twice per retry.
let lastWorkerError = null;
worker.on('error', (err) => {
  if (err.message === lastWorkerError) return;
  lastWorkerError = err.message;
  console.error(`worker: ${err.message}`);
});

console.log('ashgrove worker consuming the "orders" queue');

async function shutdown() {
  closing = true;
  await worker.close();
  connection.disconnect();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
