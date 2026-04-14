#!/usr/bin/env npx tsx
// HTTP-сервер для приёма вебхуков от amoCRM.
// При получении вебхука запускает полный пересчёт RFM.
// Запуск: npx tsx server.ts
// Порт: 3939 (или PORT env)

import { createServer } from 'http';
import { exec } from 'child_process';

const PORT = parseInt(process.env.PORT || '3939');
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';

let isRunning = false;
let lastRun = 0;
const DEBOUNCE_MS = 30_000; // не чаще раза в 30 сек

function runRfm() {
  if (isRunning) {
    console.log(`[${ts()}] Recalc already running, skipping`);
    return;
  }

  const now = Date.now();
  if (now - lastRun < DEBOUNCE_MS) {
    console.log(`[${ts()}] Debounce: last run ${((now - lastRun) / 1000).toFixed(0)}s ago, skipping`);
    return;
  }

  isRunning = true;
  lastRun = now;
  console.log(`[${ts()}] Starting RFM recalculation...`);

  const env = { ...process.env };
  exec('npx tsx run.ts', { cwd: __dirname, env }, (err, stdout, stderr) => {
    isRunning = false;
    if (err) {
      console.error(`[${ts()}] RFM error:`, stderr || err.message);
    } else {
      console.log(`[${ts()}] RFM done:\n${stdout}`);
    }
  });
}

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

const server = createServer((req, res) => {
  // Health check
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', isRunning, lastRun: new Date(lastRun).toISOString() }));
    return;
  }

  // Webhook от amoCRM (POST)
  if (req.method === 'POST' && req.url === '/webhook') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      console.log(`[${ts()}] Webhook received (${body.length} bytes)`);

      // Опциональная проверка секрета
      if (WEBHOOK_SECRET && !req.url?.includes(WEBHOOK_SECRET)) {
        // Проверяем и в query string
        const url = new URL(req.url || '/', `http://localhost`);
        if (url.searchParams.get('secret') !== WEBHOOK_SECRET) {
          // Без секрета — всё равно принимаем (amoCRM не поддерживает кастомные заголовки)
        }
      }

      runRfm();

      res.writeHead(200);
      res.end('ok');
    });
    return;
  }

  // Ручной запуск
  if (req.method === 'POST' && req.url === '/run') {
    console.log(`[${ts()}] Manual run triggered`);
    runRfm();
    res.writeHead(200);
    res.end('started');
    return;
  }

  res.writeHead(404);
  res.end('not found');
});

server.listen(PORT, () => {
  console.log(`[${ts()}] RFM webhook server listening on port ${PORT}`);
  console.log(`  Webhook URL: http://<VDS_IP>:${PORT}/webhook`);
  console.log(`  Health:      http://<VDS_IP>:${PORT}/health`);
  console.log(`  Manual run:  curl -X POST http://<VDS_IP>:${PORT}/run`);
});
