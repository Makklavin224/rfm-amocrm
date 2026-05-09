#!/usr/bin/env npx tsx
// Точечный пересчёт RFM по списку customer_id (использует кэш порогов).
// Источник списка: либо duplicates-*.json (берётся "главный" из каждой группы),
// либо напрямую CSV из ID через --ids 1,2,3.
//
// Usage:
//   npx tsx run-targeted.ts --json duplicates-1777957006675.json
//   npx tsx run-targeted.ts --ids 792463,794505

import { readFileSync } from 'fs';
import { SEGMENT_IDS, updateContactSegments } from './lib/amocrm';
import { calculateSingleSegment } from './lib/rfm';
import { loadThresholds } from './lib/thresholds';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function pickMain(cs: any[]): any {
  return [...cs].sort((a, b) => {
    if (b.purchases !== a.purchases) return b.purchases - a.purchases;
    return a.createdAt - b.createdAt;
  })[0];
}

async function main() {
  const t0 = Date.now();
  const ts = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;

  const jsonIdx = process.argv.indexOf('--json');
  const idsIdx = process.argv.indexOf('--ids');
  let ids: number[] = [];
  if (jsonIdx >= 0 && process.argv[jsonIdx + 1]) {
    const groups = JSON.parse(readFileSync(process.argv[jsonIdx + 1], 'utf-8'));
    for (const g of groups) ids.push(pickMain(g.customers).id);
  } else if (idsIdx >= 0 && process.argv[idsIdx + 1]) {
    ids = process.argv[idsIdx + 1].split(',').map((s) => parseInt(s.trim()));
  } else {
    console.error('Usage: npx tsx run-targeted.ts --json file.json | --ids 1,2,3');
    process.exit(1);
  }

  console.log(`[${ts()}] Targeted recalc for ${ids.length} customers`);
  const thr = loadThresholds();
  if (!thr) { console.error('No cached thresholds — run run.ts first'); process.exit(1); }
  console.log(`[${ts()}] Using thresholds (base=${thr.baseSize}, updated=${thr.updatedAt})`);

  const segPatch: Array<{ id: number; _embedded: { segments: [{ id: number }] } }> = [];
  const contactUpdates: Array<{ contactId: number; segment: string }> = [];

  for (const cid of ids) {
    // 1. Покупатель + контакт
    const r = await fetch(`${BASE_URL}/api/v4/customers/${cid}?with=contacts`, { headers });
    if (!r.ok) { console.warn(`  ${cid}: fetch ${r.status}`); continue; }
    const c = await r.json();
    const ltv = c.ltv || 0;
    const purchases = c.purchases_count || 0;
    if (!ltv || !purchases) { console.warn(`  ${cid}: empty (skip)`); continue; }

    const main = (c._embedded?.contacts || []).find((x: any) => x.is_main) || c._embedded?.contacts?.[0];
    if (!main) { console.warn(`  ${cid}: no contact`); continue; }
    const contactId = main.id;

    // 2. Даты первой и последней транзакции в окне 2 года
    const cutoffSec = Math.floor((Date.now() - 730 * 86400 * 1000) / 1000);
    let lastAt = 0;
    let firstAt = 0;
    let page = 1;
    while (true) {
      const tr = await fetch(`${BASE_URL}/api/v4/customers/${cid}/transactions?limit=250&page=${page}`, { headers });
      if (tr.status === 204 || !tr.ok) break;
      const d = await tr.json();
      for (const t of d._embedded?.transactions || []) {
        if (t.completed_at < cutoffSec) continue;
        if (t.completed_at > lastAt) lastAt = t.completed_at;
        if (firstAt === 0 || t.completed_at < firstAt) firstAt = t.completed_at;
      }
      if (!d._links?.next) break;
      page++;
      await sleep(200);
    }
    if (!lastAt) { console.warn(`  ${cid}: no txns`); continue; }

    // 3. Сегмент
    const { segment, daysSince, revPct, freqPct } = calculateSingleSegment(ltv, purchases, lastAt, thr.revenues, thr.frequencies, firstAt);
    const segId = SEGMENT_IDS[segment];
    console.log(`  ${cid} (contact ${contactId}) ltv=${ltv} purchases=${purchases} days=${daysSince} → "${segment}"`);

    segPatch.push({ id: cid, _embedded: { segments: [{ id: segId }] } });
    contactUpdates.push({ contactId, segment });

    await sleep(400);
  }

  // 4. Батч PATCH покупателей
  console.log(`\n[${ts()}] Patching ${segPatch.length} customer segments...`);
  for (let i = 0; i < segPatch.length; i += 50) {
    const batch = segPatch.slice(i, i + 50);
    const r = await fetch(`${BASE_URL}/api/v4/customers`, { method: 'PATCH', headers, body: JSON.stringify(batch) });
    if (!r.ok) { console.error(`  customers PATCH ${r.status}: ${(await r.text()).slice(0, 200)}`); }
    await sleep(500);
  }

  // 5. Обновление контактов (поле + тег) — переиспользуем lib
  console.log(`[${ts()}] Updating ${contactUpdates.length} contacts...`);
  const updated = await updateContactSegments(contactUpdates);
  console.log(`[${ts()}] Done. Contacts updated: ${updated}`);
}

main().catch((e) => { console.error('FAILED:', e); process.exit(1); });
