#!/usr/bin/env npx tsx
// Поиск и удаление транзакций-сирот (дубликатов).
// Сирота = транзакция БЕЗ "[DEAL:...]" в комменте, у которой есть пара
// с тем же price и той же датой (UTC день), у которой "[DEAL:...]" есть.
//
// Usage:
//   npx tsx dedupe-transactions.ts            # dry-run
//   npx tsx dedupe-transactions.ts --apply    # реально удалить

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const APPLY = process.argv.includes('--apply');

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url: string, init?: RequestInit, maxRetries = 6): Promise<Response> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await fetch(url, init);
      if (res.status !== 429 && res.status !== 403) return res;
      await sleep(2000 * Math.pow(2, attempt));
    } catch (err: any) {
      await sleep(2000 * Math.pow(2, attempt));
      if (attempt === maxRetries - 1) throw err;
    }
  }
  return fetch(url, init);
}

interface Tx {
  id: number;
  price: number;
  completed_at: number;
  comment: string;
}

interface Orphan {
  customerId: number;
  customerName: string;
  orphan: Tx;
  pairedWith: Tx;
  reason: string;
}

async function fetchAllCustomers(): Promise<Array<{ id: number; name: string }>> {
  const out: Array<{ id: number; name: string }> = [];
  let page = 1;
  while (true) {
    const res = await fetchWithRetry(
      `${BASE_URL}/api/v4/customers?limit=250&page=${page}`,
      { headers }
    );
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();
    for (const c of data?._embedded?.customers || []) {
      if (c.purchases_count && c.purchases_count > 0) {
        out.push({ id: c.id, name: c.name || '' });
      }
    }
    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }
  return out;
}

async function fetchAllTx(customerId: number): Promise<Tx[]> {
  const out: Tx[] = [];
  let page = 1;
  while (true) {
    const res = await fetchWithRetry(
      `${BASE_URL}/api/v4/customers/${customerId}/transactions?limit=250&page=${page}`,
      { headers }
    );
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();
    const items = data?._embedded?.transactions || [];
    for (const t of items) {
      out.push({
        id: t.id,
        price: t.price,
        completed_at: t.completed_at,
        comment: t.comment || '',
      });
    }
    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }
  return out;
}

function dayKey(unix: number): string {
  return new Date(unix * 1000).toISOString().split('T')[0];
}

function getDealId(comment: string): number | null {
  const m = comment.match(/\[DEAL:(\d+)\]/);
  return m ? parseInt(m[1]) : null;
}

function findOrphans(txns: Tx[]): Array<{ orphan: Tx; pairedWith: Tx; reason: string }> {
  const result: Array<{ orphan: Tx; pairedWith: Tx; reason: string }> = [];

  // 1) Дубли с одинаковым [DEAL:N] — оставляем самую раннюю по id (стабильно), остальные на удаление
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
    const keep = sorted[0];
    for (const dup of sorted.slice(1)) {
      result.push({ orphan: dup, pairedWith: keep, reason: `dup [DEAL:${dealId}]` });
    }
  }
  const toDeleteIds = new Set(result.map((r) => r.orphan.id));

  // 2) Сироты без [DEAL:] совпадающие по дате+цене с транзакцией с [DEAL:]
  const buckets = new Map<string, Tx[]>();
  for (const t of txns) {
    if (toDeleteIds.has(t.id)) continue;
    const key = `${dayKey(t.completed_at)}|${t.price}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(t);
  }
  for (const group of Array.from(buckets.values())) {
    if (group.length < 2) continue;
    const withDeal = group.filter((t) => getDealId(t.comment) !== null);
    const without = group.filter((t) => getDealId(t.comment) === null);
    if (withDeal.length === 0 || without.length === 0) continue;
    for (const orphan of without) {
      result.push({ orphan, pairedWith: withDeal[0], reason: 'orphan (same day+price)' });
    }
  }

  return result;
}

async function deleteTx(customerId: number, txId: number): Promise<boolean> {
  const res = await fetchWithRetry(
    `${BASE_URL}/api/v4/customers/${customerId}/transactions/${txId}`,
    { method: 'DELETE', headers }
  );
  if (res.ok || res.status === 204) return true;
  const text = await res.text();
  console.error(`  DELETE failed for tx ${txId}: ${res.status} ${text.slice(0, 150)}`);
  return false;
}

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${ts()}] Mode: ${APPLY ? 'APPLY (will delete)' : 'DRY-RUN (no changes)'}`);
  console.log(`[${ts()}] Fetching customers...`);
  const customers = await fetchAllCustomers();
  console.log(`[${ts()}] ${customers.length} customers with purchases`);

  const allOrphans: Orphan[] = [];
  let processed = 0;

  for (const c of customers) {
    processed++;
    if (processed % 100 === 0) {
      console.log(`[${ts()}] Processed ${processed}/${customers.length} (orphans found: ${allOrphans.length})`);
    }
    try {
      const txns = await fetchAllTx(c.id);
      const orphans = findOrphans(txns);
      for (const o of orphans) {
        allOrphans.push({ customerId: c.id, customerName: c.name, ...o });
      }
    } catch (err: any) {
      console.warn(`  Skip customer ${c.id}: ${err.message}`);
    }
    await sleep(120);
  }

  console.log(`\n[${ts()}] === Found ${allOrphans.length} orphan transaction(s) ===\n`);

  for (const o of allOrphans.slice(0, 50)) {
    const d = dayKey(o.orphan.completed_at);
    console.log(
      `  cust ${o.customerId} (${o.customerName}): tx ${o.orphan.id} [${d}] ${o.orphan.price}₽ ` +
        `→ ${o.reason}, keep tx ${o.pairedWith.id} (${(o.pairedWith.comment || '').slice(0, 40)})`
    );
  }
  if (allOrphans.length > 50) {
    console.log(`  ... and ${allOrphans.length - 50} more`);
  }

  if (!APPLY) {
    console.log(`\n[${ts()}] DRY-RUN finished. Re-run with --apply to delete.`);
    return;
  }

  console.log(`\n[${ts()}] Deleting ${allOrphans.length} orphans...`);
  let ok = 0;
  let fail = 0;
  for (const o of allOrphans) {
    const res = await deleteTx(o.customerId, o.orphan.id);
    if (res) ok++;
    else fail++;
    await sleep(200);
  }
  console.log(`\n[${ts()}] Done. Deleted: ${ok}, failed: ${fail}`);
  console.log(`Next: run \`npx tsx run.ts\` to recalculate segments on cleaned data.`);
}

main().catch((err) => {
  console.error('FAILED:', err);
  process.exit(1);
});
