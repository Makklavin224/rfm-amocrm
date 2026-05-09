#!/usr/bin/env npx tsx
// Полный пересчёт трёх кастомных полей у всех покупателей:
//   "Сумма покупок (последние 2 года)"
//   "Кол-во покупок (последние 2 года)"
//   "Средний чек (последние 2 года)"
// Окно — 730 дней от текущей даты (скользящее, дёргать ежедневно через cron).
//
// Usage:
//   npx tsx recalc-2y-fields.ts            # dry-run, показать что будет
//   npx tsx recalc-2y-fields.ts --apply    # применить (PATCH каждого покупателя)

import { calcTwoYearStats, cutoffTimestamp, Tx } from './lib/two-year-stats';
import { CUSTOMER_FIELD_IDS, buildTwoYearFieldsPatch } from './lib/customer-fields';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const APPLY = process.argv.includes('--apply');
const CONC = 3;

const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url: string, init?: RequestInit, max = 6): Promise<Response> {
  for (let i = 0; i < max; i++) {
    try {
      const res = await fetch(url, init);
      if (res.status !== 429 && res.status !== 403) return res;
      const wait = 2000 * Math.pow(2, i);
      console.warn(`  ${res.status}, wait ${wait}ms (${i + 1}/${max})`);
      await sleep(wait);
    } catch (e: any) {
      const wait = 2000 * Math.pow(2, i);
      console.warn(`  net err ${e.code || ''}, wait ${wait}ms`);
      await sleep(wait);
    }
  }
  return fetch(url, init);
}

interface CustRow { id: number; name: string; ltv: number; purchases: number; }

async function fetchAllCustomers(): Promise<CustRow[]> {
  const out: CustRow[] = [];
  let page = 1;
  while (true) {
    const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers?limit=250&page=${page}`, { headers });
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const c of d._embedded?.customers || []) {
      out.push({ id: c.id, name: c.name || '', ltv: c.ltv || 0, purchases: c.purchases_count || 0 });
    }
    if (!d._links?.next) break;
    page++;
    await sleep(150);
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

async function main() {
  const t0 = Date.now();
  const ts = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;
  const now = new Date();
  const cutoff = cutoffTimestamp(now);

  console.log(`[${ts()}] Mode: ${APPLY ? 'APPLY' : 'DRY-RUN'}`);
  console.log(`[${ts()}] Window: from ${new Date(cutoff * 1000).toISOString().split('T')[0]} to ${now.toISOString().split('T')[0]}`);
  console.log(`[${ts()}] Field IDs: sum=${CUSTOMER_FIELD_IDS.TWO_YEAR_SUM} count=${CUSTOMER_FIELD_IDS.TWO_YEAR_COUNT} avg=${CUSTOMER_FIELD_IDS.TWO_YEAR_AVG}`);

  console.log(`[${ts()}] Fetching all customers...`);
  const customers = await fetchAllCustomers();
  console.log(`[${ts()}] Total customers: ${customers.length}`);

  // Накапливаем апдейты, патчим батчами по 50
  const updates: Array<{ customerId: number; sum: number; count: number; avg: number }> = [];
  let processed = 0;
  let withWindow = 0;

  for (let i = 0; i < customers.length; i += CONC) {
    const batch = customers.slice(i, i + CONC);
    const results = await Promise.all(
      batch.map(async (c) => {
        try {
          const txns = await fetchAllTx(c.id);
          const stats = calcTwoYearStats(txns, now);
          return { id: c.id, stats };
        } catch (e: any) {
          console.warn(`  customer ${c.id} fail: ${e.message}`);
          return null;
        }
      })
    );
    for (const r of results) {
      if (!r) continue;
      updates.push({ customerId: r.id, sum: r.stats.sum, count: r.stats.count, avg: r.stats.avg });
      if (r.stats.count > 0) withWindow++;
    }
    processed += batch.length;
    if (processed % 500 === 0 || processed >= customers.length) {
      console.log(`[${ts()}] Processed ${processed}/${customers.length} (with-window=${withWindow})`);
    }
    await sleep(200);
  }

  console.log(`\n[${ts()}] Stats:`);
  console.log(`  Total: ${updates.length}`);
  console.log(`  With txns in window (count>0): ${withWindow}`);
  console.log(`  Empty in window: ${updates.length - withWindow}`);

  // Топ-5 для проверки глазами
  const top = [...updates].sort((a, b) => b.sum - a.sum).slice(0, 5);
  console.log(`\n  Top 5 by 2y-sum:`);
  for (const u of top) console.log(`    customer ${u.customerId}: sum=${u.sum} count=${u.count} avg=${u.avg}`);

  if (!APPLY) {
    console.log(`\n[${ts()}] DRY-RUN — to apply add --apply`);
    return;
  }

  // PATCH батчами по 50
  console.log(`\n[${ts()}] Patching ${updates.length} customers...`);
  let patched = 0;
  let failed = 0;
  for (let i = 0; i < updates.length; i += 50) {
    const chunk = updates.slice(i, i + 50);
    const body = chunk.map((u) => ({
      id: u.customerId,
      custom_fields_values: buildTwoYearFieldsPatch(u.sum, u.count, u.avg),
    }));
    const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers`, {
      method: 'PATCH',
      headers,
      body: JSON.stringify(body),
    });
    if (r.ok) {
      patched += chunk.length;
    } else {
      failed += chunk.length;
      const t = await r.text();
      console.error(`  PATCH ${r.status}: ${t.slice(0, 200)}`);
    }
    if ((i + 50) % 500 === 0 || i + 50 >= updates.length) {
      console.log(`[${ts()}] Patched ${patched}/${updates.length} (failed=${failed})`);
    }
    await sleep(400);
  }

  console.log(`\n[${ts()}] Done. Patched=${patched}, Failed=${failed}`);
}

main().catch((e) => { console.error('FAILED:', e); process.exit(1); });
