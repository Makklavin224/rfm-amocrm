#!/usr/bin/env npx tsx
// Удаление "пустых" покупателей: purchases_count = 0 И ltv = 0.
// Это карточки, созданные авто-триггером amoCRM из чужих воронок.
//
// БЕЗОПАСНОСТЬ:
//   - dry-run по умолчанию
//   - сохраняет ID удаляемых в empty-customers-backup.json ПЕРЕД удалением
//   - чанки по 50, можно прервать Ctrl+C
//
// Usage:
//   npx tsx cleanup-empty-customers.ts            # dry-run
//   npx tsx cleanup-empty-customers.ts --apply    # реально удалить

import { writeFileSync } from 'fs';

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

interface Empty {
  id: number;
  name: string;
  contactIds: number[];
}

async function findEmpty(): Promise<Empty[]> {
  const out: Empty[] = [];
  let page = 1;
  while (true) {
    const res = await fetchWithRetry(
      `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`,
      { headers }
    );
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();
    for (const c of data?._embedded?.customers || []) {
      const purch = c.purchases_count || 0;
      const ltv = c.ltv || 0;
      if (purch === 0 && ltv === 0) {
        out.push({
          id: c.id,
          name: c.name || '',
          contactIds: (c._embedded?.contacts || []).map((x: any) => x.id),
        });
      }
    }
    if (!data._links?.next) break;
    page++;
    await sleep(120);
  }
  return out;
}

async function deleteCustomersBatch(ids: number[]): Promise<boolean> {
  // amoCRM v4 поддерживает DELETE с body массивом
  const res = await fetchWithRetry(`${BASE_URL}/api/v4/customers`, {
    method: 'DELETE',
    headers,
    body: JSON.stringify(ids.map((id) => ({ id }))),
  });
  if (res.ok || res.status === 204) return true;
  const text = await res.text();
  console.error(`  Batch DELETE failed (${ids.length} ids): ${res.status} ${text.slice(0, 200)}`);
  return false;
}

async function deleteCustomerOne(id: number): Promise<boolean> {
  const res = await fetchWithRetry(`${BASE_URL}/api/v4/customers/${id}`, {
    method: 'DELETE',
    headers,
  });
  if (res.ok || res.status === 204) return true;
  return false;
}

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${ts()}] Mode: ${APPLY ? 'APPLY (will delete)' : 'DRY-RUN (no changes)'}`);
  console.log(`[${ts()}] Scanning customers for purchases_count=0 AND ltv=0...`);

  const empty = await findEmpty();
  console.log(`[${ts()}] Found ${empty.length} empty customer(s)\n`);

  // Превью первых 20
  for (const e of empty.slice(0, 20)) {
    console.log(`  cust ${e.id}  "${e.name}"  contacts: [${e.contactIds.join(', ')}]`);
  }
  if (empty.length > 20) console.log(`  ... and ${empty.length - 20} more\n`);

  // Бэкап ID в файл
  const backupPath = `${__dirname}/empty-customers-backup-${Date.now()}.json`;
  writeFileSync(backupPath, JSON.stringify(empty, null, 2));
  console.log(`[${ts()}] Backup written: ${backupPath}`);

  if (!APPLY) {
    console.log(`\n[${ts()}] DRY-RUN finished. Re-run with --apply to delete.`);
    return;
  }

  console.log(`\n[${ts()}] Deleting ${empty.length} empty customers (batches of 50)...`);
  let ok = 0;
  let fail = 0;

  for (let i = 0; i < empty.length; i += 50) {
    const batch = empty.slice(i, i + 50);
    const ids = batch.map((e) => e.id);
    const success = await deleteCustomersBatch(ids);

    if (success) {
      ok += ids.length;
    } else {
      // Fallback: пробуем по одному
      console.warn(`  Batch failed, falling back to one-by-one for ${ids.length} ids...`);
      for (const id of ids) {
        const r = await deleteCustomerOne(id);
        if (r) ok++;
        else fail++;
        await sleep(150);
      }
    }

    if ((i + 50) % 500 === 0 || i + 50 >= empty.length) {
      console.log(`[${ts()}] Progress: ${ok + fail}/${empty.length}  (ok=${ok}, fail=${fail})`);
    }
    await sleep(300);
  }

  console.log(`\n[${ts()}] Done. Deleted: ${ok}, failed: ${fail}`);
}

main().catch((err) => {
  console.error('FAILED:', err);
  process.exit(1);
});
