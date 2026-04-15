#!/usr/bin/env npx tsx
// HTTP-сервер для приёма вебхуков от amoCRM.
// При получении вебхука запускает быстрый пересчёт ОДНОЙ сделки (run-single.ts).
// Полный пересчёт — через cron в 02:00.
// Порт: 3939 (или PORT env)

import { createServer } from 'http';
import { exec } from 'child_process';

const PORT = parseInt(process.env.PORT || '3939');

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// Распарсить form-encoded webhook body amoCRM
function parseLeadId(body: string): number | null {
  // amoCRM отправляет form-encoded: leads[status][0][id]=123456
  const decoded = decodeURIComponent(body.replace(/\+/g, ' '));
  const m = decoded.match(/leads\[status\]\[\d+\]\[id\]=(\d+)/);
  if (m) return parseInt(m[1]);

  // Ещё одна форма: leads[add][0][id]=...
  const m2 = decoded.match(/leads\[\w+\]\[\d+\]\[id\]=(\d+)/);
  if (m2) return parseInt(m2[1]);

  return null;
}

function runSingleRecalc(leadId: number) {
  const env = { ...process.env };
  console.log(`[${ts()}] Running single recalc for lead #${leadId}`);

  exec(`npx tsx run-single.ts ${leadId}`, { cwd: __dirname, env }, (err, stdout, stderr) => {
    if (err) {
      console.error(`[${ts()}] Single recalc error: ${stderr || err.message}`);
    } else {
      console.log(`[${ts()}] Single recalc done:\n${stdout}`);
    }
  });
}

function runFullRecalc() {
  const env = { ...process.env };
  console.log(`[${ts()}] Running FULL recalc`);

  exec('npx tsx run.ts', { cwd: __dirname, env }, (err, stdout, stderr) => {
    if (err) {
      console.error(`[${ts()}] Full recalc error: ${stderr || err.message}`);
    } else {
      console.log(`[${ts()}] Full recalc done:\n${stdout.slice(-1500)}`);
    }
  });
}

const server = createServer((req, res) => {
  // Health check
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  // Webhook от amoCRM
  if (req.method === 'POST' && req.url === '/webhook') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      const leadId = parseLeadId(body);
      console.log(`[${ts()}] Webhook received (${body.length} bytes), lead=${leadId || 'unknown'}`);

      if (leadId) {
        runSingleRecalc(leadId);
      } else {
        console.warn(`[${ts()}] Could not parse lead_id from webhook. Body: ${body.slice(0, 200)}`);
      }

      res.writeHead(200);
      res.end('ok');
    });
    return;
  }

  // Ручной полный пересчёт
  if (req.method === 'POST' && req.url === '/run-full') {
    console.log(`[${ts()}] Manual FULL recalc triggered`);
    runFullRecalc();
    res.writeHead(200);
    res.end('started');
    return;
  }

  // Ручной одиночный пересчёт
  if (req.method === 'POST' && req.url?.startsWith('/run-single/')) {
    const leadId = parseInt(req.url.split('/').pop() || '');
    if (leadId) {
      runSingleRecalc(leadId);
      res.writeHead(200);
      res.end(`started for lead ${leadId}`);
    } else {
      res.writeHead(400);
      res.end('invalid lead_id');
    }
    return;
  }

  res.writeHead(404);
  res.end('not found');
});

server.listen(PORT, () => {
  console.log(`[${ts()}] RFM webhook server listening on port ${PORT}`);
  console.log(`  Webhook:       POST http://<VDS_IP>:${PORT}/webhook`);
  console.log(`  Full recalc:   POST http://<VDS_IP>:${PORT}/run-full`);
  console.log(`  Single recalc: POST http://<VDS_IP>:${PORT}/run-single/<lead_id>`);
  console.log(`  Health:        GET  http://<VDS_IP>:${PORT}/health`);
});
