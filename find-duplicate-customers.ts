#!/usr/bin/env npx tsx
// Находит контакты, у которых привязано >1 покупателя (Customer).
// Сохраняет результат в duplicates-<ts>.json
//
// Usage: npx tsx find-duplicate-customers.ts

import { writeFileSync } from 'fs';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
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

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  // contactId → customers
  const byContact = new Map<number, Cust[]>();
  const noContact: Cust[] = [];

  let page = 1;
  let total = 0;
  while (true) {
    const r = await fetchWithRetry(
      `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`,
      { headers }
    );
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    const items = d._embedded?.customers || [];
    for (const c of items) {
      const cust: Cust = {
        id: c.id,
        name: c.name || '',
        ltv: c.ltv || 0,
        purchases: c.purchases_count || 0,
        createdAt: c.created_at || 0,
      };
      const contacts = c._embedded?.contacts || [];
      if (contacts.length === 0) {
        noContact.push(cust);
        continue;
      }
      // Группируем по основному контакту (is_main, иначе первый)
      const main = contacts.find((x: any) => x.is_main) || contacts[0];
      const cid = main.id;
      if (!byContact.has(cid)) byContact.set(cid, []);
      byContact.get(cid)!.push(cust);
    }
    total += items.length;
    if (!d._links?.next) break;
    page++;
    if (page % 5 === 0) console.log(`[${ts()}] scanned ${total} customers...`);
    await sleep(200);
  }

  console.log(`[${ts()}] Total scanned: ${total}`);
  console.log(`[${ts()}] Customers without contact: ${noContact.length}`);

  // Дубли
  const dups = Array.from(byContact.entries()).filter(([, cs]) => cs.length > 1);
  console.log(`[${ts()}] Contacts with >1 customer: ${dups.length}`);

  const totalDupCustomers = dups.reduce((s, [, cs]) => s + cs.length, 0);
  const wouldRemove = dups.reduce((s, [, cs]) => s + (cs.length - 1), 0);
  console.log(`[${ts()}] Total customers in dup groups: ${totalDupCustomers} (would merge → ${wouldRemove} to remove)`);

  // Top examples
  console.log(`\nTop 10 dup groups by total LTV:`);
  const sorted = [...dups].sort((a, b) => {
    const sa = a[1].reduce((s, c) => s + c.ltv, 0);
    const sb = b[1].reduce((s, c) => s + c.ltv, 0);
    return sb - sa;
  });
  for (const [cid, cs] of sorted.slice(0, 10)) {
    console.log(`  contact ${cid}:`);
    for (const c of cs) {
      console.log(`    customer ${c.id} "${c.name}" ltv=${c.ltv} purchases=${c.purchases}`);
    }
  }

  const out = dups.map(([contactId, cs]) => ({ contactId, customers: cs }));
  const path = `${__dirname}/duplicates-${Date.now()}.json`;
  writeFileSync(path, JSON.stringify(out, null, 2));
  console.log(`\n[${ts()}] Saved → ${path}`);
}

main().catch((e) => { console.error('FAILED:', e); process.exit(1); });
