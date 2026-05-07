#!/usr/bin/env npx tsx
// Удаляет ВСЕ транзакции у покупателей-дублей (после того как они уже
// перенесены на главного через merge-duplicates.ts).
// После этого у дубля будет purchases=0, ltv=0 — RFM его проигнорирует.
//
// Usage:
//   npx tsx empty-duplicates.ts <duplicates.json>            # dry-run
//   npx tsx empty-duplicates.ts <duplicates.json> --apply

import { readFileSync } from 'fs';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const APPLY = process.argv.includes('--apply');
const inputFile = process.argv.find((a) => a.endsWith('.json'));
if (!inputFile) { console.error('Usage'); process.exit(1); }

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

interface Cust { id: number; name: string; ltv: number; purchases: number; createdAt: number; }
interface Group { contactId: number; customers: Cust[]; }

function pickMain(cs: Cust[]): Cust {
  return [...cs].sort((a, b) => {
    if (b.purchases !== a.purchases) return b.purchases - a.purchases;
    return a.createdAt - b.createdAt;
  })[0];
}

async function main() {
  const groups: Group[] = JSON.parse(readFileSync(inputFile!, 'utf-8'));
  console.log(`Mode: ${APPLY ? 'APPLY' : 'DRY-RUN'}`);

  let totalToDelete = 0, deleted = 0, failed = 0;

  for (const g of groups) {
    const main = pickMain(g.customers);
    const dups = g.customers.filter((c) => c.id !== main.id);
    for (const dup of dups) {
      // фетч транзакций
      const txIds: number[] = [];
      let page = 1;
      while (true) {
        const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${dup.id}/transactions?limit=250&page=${page}`, { headers });
        if (r.status === 204 || !r.ok) break;
        const d = await r.json();
        for (const t of d._embedded?.transactions || []) txIds.push(t.id);
        if (!d._links?.next) break;
        page++;
        await sleep(300);
      }
      console.log(`dup ${dup.id} (contact ${g.contactId}): ${txIds.length} txns to delete`);
      totalToDelete += txIds.length;

      if (APPLY) {
        for (const tid of txIds) {
          const r = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${dup.id}/transactions/${tid}`, { method: 'DELETE', headers });
          if (r.ok || r.status === 204) deleted++;
          else { failed++; console.warn(`  fail tx ${tid}: ${r.status}`); }
          await sleep(500);
        }
      }
      await sleep(500);
    }
  }

  console.log(`\n=== Summary ===`);
  console.log(`  Total tx to delete: ${totalToDelete}`);
  if (APPLY) console.log(`  Deleted: ${deleted}, Failed: ${failed}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
