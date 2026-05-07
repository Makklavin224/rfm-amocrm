#!/usr/bin/env npx tsx
// Сливает дублирующих покупателей: для каждого контакта с >1 customer
// оставляет главного (max purchases, при равенстве — старший по created_at),
// переносит транзакции с дублей (без deal_id-конфликта), удаляет дубли.
//
// Usage:
//   npx tsx merge-duplicates.ts <duplicates-*.json>            # dry-run
//   npx tsx merge-duplicates.ts <duplicates-*.json> --apply    # применить

import { readFileSync } from 'fs';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const APPLY = process.argv.includes('--apply');
const inputFile = process.argv.find((a) => a.endsWith('.json'));
if (!inputFile) {
  console.error('Usage: npx tsx merge-duplicates.ts <duplicates.json> [--apply]');
  process.exit(1);
}

const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url: string, init?: RequestInit, max = 6): Promise<Response> {
  for (let i = 0; i < max; i++) {
    try {
      const res = await fetch(url, init);
      if (res.status !== 429 && res.status !== 403) return res;
      const wait = 2000 * Math.pow(2, i);
      console.warn(`    ${res.status}, wait ${wait}ms (${i + 1}/${max})`);
      await sleep(wait);
    } catch (e: any) {
      const wait = 2000 * Math.pow(2, i);
      console.warn(`    network err ${e.code || ''}, wait ${wait}ms`);
      await sleep(wait);
    }
  }
  return fetch(url, init);
}

interface Cust { id: number; name: string; ltv: number; purchases: number; createdAt: number; }
interface Group { contactId: number; customers: Cust[]; }
interface Tx { id: number; price: number; completed_at: number; comment: string; }

const getDealId = (c: string) => { const m = (c || '').match(/\[DEAL:(\d+)\]/); return m ? parseInt(m[1]) : null; };

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
    await sleep(400);
  }
  return out;
}

async function createTx(custId: number, tx: Tx): Promise<boolean> {
  const body = [{ price: tx.price, completed_at: tx.completed_at, comment: tx.comment }];
  const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${custId}/transactions`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const t = await r.text();
    console.warn(`    create tx fail: ${r.status} ${t.slice(0, 150)}`);
    return false;
  }
  return true;
}

async function deleteCustomer(id: number): Promise<boolean> {
  // amoCRM удаляет покупателя через DELETE с filter[id][]
  const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers?filter[id][]=${id}`, {
    method: 'DELETE',
    headers,
  });
  return r.ok || r.status === 204;
}

function pickMain(cs: Cust[]): Cust {
  return [...cs].sort((a, b) => {
    if (b.purchases !== a.purchases) return b.purchases - a.purchases;
    return a.createdAt - b.createdAt; // старший = меньше createdAt
  })[0];
}

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  const groups: Group[] = JSON.parse(readFileSync(inputFile!, 'utf-8'));
  console.log(`[${ts()}] Mode: ${APPLY ? 'APPLY' : 'DRY-RUN'}`);
  console.log(`[${ts()}] Groups to process: ${groups.length}\n`);

  let mergedTxs = 0;
  let skippedTxs = 0;
  let deletedCustomers = 0;
  let failedCreates = 0;
  let failedDeletes = 0;

  for (const g of groups) {
    const main = pickMain(g.customers);
    const dups = g.customers.filter((c) => c.id !== main.id);
    console.log(`\n[${ts()}] contact ${g.contactId}: main=${main.id} (purchases=${main.purchases} ltv=${main.ltv}), dups=[${dups.map((d) => d.id).join(',')}]`);

    // Существующие dealId у главного (чтобы не дублировать)
    const mainTxns = await fetchAllTx(main.id);
    const mainDeals = new Set<number>();
    for (const t of mainTxns) {
      const d = getDealId(t.comment);
      if (d) mainDeals.add(d);
    }
    console.log(`    main has ${mainTxns.length} txns, ${mainDeals.size} unique dealIds`);

    for (const dup of dups) {
      const dupTxns = await fetchAllTx(dup.id);
      console.log(`    dup ${dup.id}: ${dupTxns.length} txns`);

      const toCopy: Tx[] = [];
      for (const t of dupTxns) {
        const d = getDealId(t.comment);
        if (d && mainDeals.has(d)) {
          skippedTxs++; // полный дубль по dealId
        } else {
          toCopy.push(t);
        }
      }
      console.log(`    → ${toCopy.length} to copy, ${dupTxns.length - toCopy.length} skip (dealId clash)`);

      if (APPLY) {
        for (const t of toCopy) {
          const ok = await createTx(main.id, t);
          if (ok) {
            mergedTxs++;
            const d = getDealId(t.comment);
            if (d) mainDeals.add(d);
          } else {
            failedCreates++;
          }
          await sleep(500);
        }
        const ok = await deleteCustomer(dup.id);
        if (ok) {
          deletedCustomers++;
          console.log(`    ✓ deleted customer ${dup.id}`);
        } else {
          failedDeletes++;
          console.log(`    ✗ FAILED to delete ${dup.id}`);
        }
        await sleep(500);
      } else {
        mergedTxs += toCopy.length;
      }
    }
    await sleep(700);
  }

  console.log(`\n[${ts()}] === ${APPLY ? 'APPLIED' : 'DRY-RUN'} summary ===`);
  console.log(`  Transactions to merge: ${mergedTxs}`);
  console.log(`  Transactions skipped (dealId clash): ${skippedTxs}`);
  if (APPLY) {
    console.log(`  Customers deleted: ${deletedCustomers}`);
    console.log(`  Failed creates: ${failedCreates}`);
    console.log(`  Failed deletes: ${failedDeletes}`);
  }
}

main().catch((e) => { console.error('FAILED:', e); process.exit(1); });
