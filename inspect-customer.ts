#!/usr/bin/env npx tsx
// Диагностика: показывает все транзакции покупателя, группирует по deal_id,
// сравнивает реальные суммы с тем, что хранится в карточке amoCRM.
//
// Usage: npx tsx inspect-customer.ts <customer_id>

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const getDealId = (c: string) => {
  const m = (c || '').match(/\[DEAL:(\d+)\]/);
  return m ? parseInt(m[1]) : null;
};

async function main() {
  const id = parseInt(process.argv[2]);
  if (!id) { console.error('Usage: npx tsx inspect-customer.ts <id>'); process.exit(1); }

  const cr = await fetch(`${BASE_URL}/api/v4/customers/${id}`, { headers });
  const c = await cr.json();
  console.log(`\nCustomer ${id} — ${c.name}`);
  console.log(`  amoCRM stored:   ltv=${c.ltv}  purchases_count=${c.purchases_count}\n`);

  const txns: Array<{ id: number; price: number; completed_at: number; comment: string }> = [];
  let page = 1;
  while (true) {
    const r = await fetch(`${BASE_URL}/api/v4/customers/${id}/transactions?limit=250&page=${page}`, { headers });
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const t of d._embedded?.transactions || []) {
      txns.push({ id: t.id, price: t.price, completed_at: t.completed_at, comment: t.comment || '' });
    }
    if (!d._links?.next) break;
    page++;
    await sleep(200);
  }

  const sum = txns.reduce((s, t) => s + t.price, 0);
  console.log(`  Real transactions: count=${txns.length}  sum=${sum}\n`);

  // Группировка по deal_id
  const byDeal = new Map<string, typeof txns>();
  let noDeal = 0;
  for (const t of txns) {
    const d = getDealId(t.comment);
    const k = d ? String(d) : `__nodeal_${noDeal++}`;
    if (!byDeal.has(k)) byDeal.set(k, []);
    byDeal.get(k)!.push(t);
  }

  const dups = Array.from(byDeal.entries()).filter(([k, v]) => !k.startsWith('__nodeal') && v.length > 1);
  const orphans = txns.filter((t) => getDealId(t.comment) === null);

  console.log(`  Unique deals referenced: ${Array.from(byDeal.keys()).filter((k) => !k.startsWith('__')).length}`);
  console.log(`  Orphan tx (no [DEAL:...]): ${orphans.length}`);
  console.log(`  Duplicate-by-deal groups: ${dups.length}`);
  if (dups.length > 0) {
    console.log(`\n  Examples of duplicate dealIds:`);
    for (const [d, ts] of dups.slice(0, 10)) {
      console.log(`    DEAL:${d} → ${ts.length} txns: ${ts.map((t) => `tx${t.id}=${t.price}₽`).join(', ')}`);
    }
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
