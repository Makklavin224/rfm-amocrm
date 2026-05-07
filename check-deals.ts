#!/usr/bin/env npx tsx
// Проверяет существование deal_id из транзакций покупателя в amoCRM.
// Usage: npx tsx check-deals.ts <customer_id>

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const getDealId = (c: string) => { const m = (c || '').match(/\[DEAL:(\d+)\]/); return m ? parseInt(m[1]) : null; };

async function main() {
  const id = parseInt(process.argv[2]);
  if (!id) process.exit(1);

  const dealIds: number[] = [];
  let page = 1;
  while (true) {
    const r = await fetch(`${BASE_URL}/api/v4/customers/${id}/transactions?limit=250&page=${page}`, { headers });
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const t of d._embedded?.transactions || []) {
      const did = getDealId(t.comment || '');
      if (did) dealIds.push(did);
    }
    if (!d._links?.next) break;
    page++;
    await sleep(200);
  }
  console.log(`Total dealIds in transactions: ${dealIds.length}`);

  // Проверяем существование сделок батчами по filter[id][]
  const exists: number[] = [];
  const missing: number[] = [];
  for (let i = 0; i < dealIds.length; i += 50) {
    const batch = dealIds.slice(i, i + 50);
    const q = batch.map((d) => `filter[id][]=${d}`).join('&');
    const r = await fetch(`${BASE_URL}/api/v4/leads?${q}&limit=250`, { headers });
    if (r.ok) {
      const d = await r.json();
      const found = new Set((d._embedded?.leads || []).map((l: any) => l.id));
      for (const did of batch) (found.has(did) ? exists : missing).push(did);
    } else if (r.status === 204) {
      for (const did of batch) missing.push(did);
    }
    await sleep(400);
  }
  console.log(`  Existing leads: ${exists.length}`);
  console.log(`  Missing (deleted) leads: ${missing.length}`);
  if (missing.length) console.log(`  Examples: ${missing.slice(0, 10).join(', ')}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
