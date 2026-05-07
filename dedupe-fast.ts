#!/usr/bin/env npx tsx
// Параллельная версия дедупа: concurrency=10, ~12 мин вместо ~78.
// Сохраняет найденных сирот в orphans-<ts>.json до удаления.
//
// Usage:
//   npx tsx dedupe-fast.ts            # dry-run + сохранить JSON
//   npx tsx dedupe-fast.ts --apply    # dry-run + сохранить JSON + удалить

import { writeFileSync } from 'fs';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const APPLY = process.argv.includes('--apply');
const CONC = 10;
const SKIP = (() => {
  const i = process.argv.indexOf('--skip');
  return i >= 0 && process.argv[i + 1] ? parseInt(process.argv[i + 1]) : 0;
})();

const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url: string, init?: RequestInit, max = 6): Promise<Response> {
  for (let i = 0; i < max; i++) {
    try {
      const res = await fetch(url, init);
      if (res.status !== 429 && res.status !== 403) return res;
      await sleep(2000 * Math.pow(2, i));
    } catch {
      await sleep(2000 * Math.pow(2, i));
    }
  }
  return fetch(url, init);
}

interface Tx { id: number; price: number; completed_at: number; comment: string; }
interface OrphanRec { customerId: number; customerName: string; txId: number; reason: string; pairedWithTxId: number; }

const dayKey = (u: number) => new Date(u * 1000).toISOString().split('T')[0];
const getDealId = (c: string) => { const m = c.match(/\[DEAL:(\d+)\]/); return m ? parseInt(m[1]) : null; };

async function fetchAllCustomers(): Promise<Array<{ id: number; name: string }>> {
  const out: Array<{ id: number; name: string }> = [];
  let page = 1;
  while (true) {
    const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers?limit=250&page=${page}`, { headers });
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const c of d._embedded?.customers || []) {
      if (c.purchases_count && c.purchases_count > 0) out.push({ id: c.id, name: c.name || '' });
    }
    if (!d._links?.next) break;
    page++;
    await sleep(120);
  }
  return out;
}

async function fetchAllTx(custId: number): Promise<Tx[]> {
  const out: Tx[] = [];
  let page = 1;
  while (true) {
    const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${custId}/transactions?limit=250&page=${page}`, { headers });
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const t of d._embedded?.transactions || []) {
      out.push({ id: t.id, price: t.price, completed_at: t.completed_at, comment: t.comment || '' });
    }
    if (!d._links?.next) break;
    page++;
  }
  return out;
}

function findOrphans(txns: Tx[]): Array<{ orphanId: number; pairedWithId: number; reason: string }> {
  const result: Array<{ orphanId: number; pairedWithId: number; reason: string }> = [];

  // 1) дубли по dealId
  const byDealId = new Map<number, Tx[]>();
  for (const t of txns) {
    const did = getDealId(t.comment);
    if (did === null) continue;
    if (!byDealId.has(did)) byDealId.set(did, []);
    byDealId.get(did)!.push(t);
  }
  for (const [dealId, group] of Array.from(byDealId.entries())) {
    if (group.length < 2) continue;
    const sorted = [...group].sort((a, b) => a.id - b.id);
    for (const dup of sorted.slice(1)) {
      result.push({ orphanId: dup.id, pairedWithId: sorted[0].id, reason: `dup [DEAL:${dealId}]` });
    }
  }
  const drop = new Set(result.map((r) => r.orphanId));

  // 2) сироты без [DEAL:] парные по date+price
  const buckets = new Map<string, Tx[]>();
  for (const t of txns) {
    if (drop.has(t.id)) continue;
    const k = `${dayKey(t.completed_at)}|${t.price}`;
    if (!buckets.has(k)) buckets.set(k, []);
    buckets.get(k)!.push(t);
  }
  for (const group of Array.from(buckets.values())) {
    if (group.length < 2) continue;
    const wd = group.filter((t) => getDealId(t.comment) !== null);
    const wo = group.filter((t) => getDealId(t.comment) === null);
    if (wd.length === 0 || wo.length === 0) continue;
    for (const o of wo) result.push({ orphanId: o.id, pairedWithId: wd[0].id, reason: 'orphan-pair' });
  }

  return result;
}

async function deleteTx(custId: number, txId: number): Promise<boolean> {
  const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${custId}/transactions/${txId}`, { method: 'DELETE', headers });
  return r.ok || r.status === 204;
}

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${ts()}] Mode: ${APPLY ? 'APPLY' : 'DRY-RUN'}, concurrency=${CONC}, skip=${SKIP}`);
  console.log(`[${ts()}] Fetching customers...`);
  const allCustomers = await fetchAllCustomers();
  const customers = SKIP > 0 ? allCustomers.slice(SKIP) : allCustomers;
  console.log(`[${ts()}] Total ${allCustomers.length}, scanning ${customers.length} (skipped ${SKIP})`);
  console.log(`[${ts()}] ${customers.length} customers with purchases`);

  // Параллельный скан
  const allOrphans: OrphanRec[] = [];
  let processed = 0;

  for (let i = 0; i < customers.length; i += CONC) {
    const batch = customers.slice(i, i + CONC);
    await Promise.all(batch.map(async (c) => {
      try {
        const txns = await fetchAllTx(c.id);
        const orphans = findOrphans(txns);
        for (const o of orphans) {
          allOrphans.push({
            customerId: c.id,
            customerName: c.name,
            txId: o.orphanId,
            reason: o.reason,
            pairedWithTxId: o.pairedWithId,
          });
        }
      } catch (e: any) { /* skip */ }
      processed++;
    }));

    if (processed % 500 === 0 || processed >= customers.length) {
      console.log(`[${ts()}] ${processed}/${customers.length} (orphans=${allOrphans.length})`);
    }
  }

  // Сохраняем JSON
  const jsonPath = `${__dirname}/orphans-${Date.now()}.json`;
  writeFileSync(jsonPath, JSON.stringify(allOrphans, null, 2));
  console.log(`\n[${ts()}] Found ${allOrphans.length} orphans. Saved to ${jsonPath}`);

  if (!APPLY) {
    console.log(`[${ts()}] DRY-RUN finished. Re-run with --apply to delete.`);
    return;
  }

  // Удаляем (concurrency=10)
  console.log(`[${ts()}] Deleting...`);
  let ok = 0, fail = 0;
  for (let i = 0; i < allOrphans.length; i += CONC) {
    const batch = allOrphans.slice(i, i + CONC);
    const res = await Promise.all(batch.map((o) => deleteTx(o.customerId, o.txId)));
    for (const r of res) r ? ok++ : fail++;
    if ((i + CONC) % 200 === 0 || i + CONC >= allOrphans.length) {
      console.log(`[${ts()}] Deleted ${ok + fail}/${allOrphans.length}  (ok=${ok}, fail=${fail})`);
    }
  }

  console.log(`\n[${ts()}] Done. Deleted: ${ok}, failed: ${fail}`);
}

main().catch((e) => { console.error('FAILED:', e); process.exit(1); });
