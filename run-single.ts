#!/usr/bin/env npx tsx
// Пересчёт сегмента для ОДНОГО покупателя по ID сделки.
// Использует кэшированные перцентили из .thresholds.json.
// Usage: npx tsx run-single.ts <lead_id>

import { SEGMENT_IDS, updateContactSegments } from './lib/amocrm';
import { calculateSingleSegment } from './lib/rfm';
import { loadThresholds } from './lib/thresholds';
import { calcTwoYearStats, cutoffTimestamp, Tx } from './lib/two-year-stats';
import { buildTwoYearFieldsPatch } from './lib/customer-fields';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function main() {
  const leadId = parseInt(process.argv[2]);
  if (!leadId) {
    console.error('Usage: npx tsx run-single.ts <lead_id>');
    process.exit(1);
  }

  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${ts()}] Single recalc for lead #${leadId}`);

  // 1. Загружаем кэш перцентилей
  const thresholds = loadThresholds();
  if (!thresholds) {
    console.error('No thresholds cached. Run full `run.ts` first.');
    process.exit(1);
  }
  console.log(`[${ts()}] Thresholds: base=${thresholds.baseSize}, updated=${thresholds.updatedAt}`);

  // 2. Получаем сделку → contact_id
  const leadRes = await fetch(
    `${BASE_URL}/api/v4/leads/${leadId}?with=contacts`,
    { headers }
  );
  if (!leadRes.ok) {
    console.error(`Lead fetch failed: ${leadRes.status}`);
    process.exit(1);
  }
  const lead = await leadRes.json();
  const mainContact = lead._embedded?.contacts?.find((c: any) => c.is_main);
  if (!mainContact) {
    console.error('Lead has no main contact');
    process.exit(1);
  }
  const contactId = mainContact.id;
  console.log(`[${ts()}] Lead → contact #${contactId}`);

  // 3. Находим покупателя по контакту (через фильтр)
  const custListRes = await fetch(
    `${BASE_URL}/api/v4/customers?limit=250&with=contacts`,
    { headers }
  );
  if (!custListRes.ok) {
    console.error(`Customers fetch failed: ${custListRes.status}`);
    process.exit(1);
  }

  // Перебираем страницы, ищем покупателя по contact_id
  let customerId: number | null = null;
  let customerData: any = null;
  let page = 1;

  search: while (true) {
    const res = await fetch(
      `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`,
      { headers }
    );
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();
    for (const cust of data?._embedded?.customers || []) {
      const contacts = cust._embedded?.contacts || [];
      if (contacts.some((c: any) => c.id === contactId)) {
        customerId = cust.id;
        customerData = cust;
        break search;
      }
    }
    if (!data._links?.next) break;
    page++;
  }

  if (!customerId || !customerData) {
    console.error(`No customer found for contact #${contactId}. Full recalc may be needed.`);
    process.exit(1);
  }

  console.log(`[${ts()}] Customer #${customerId}: stored ltv=${customerData.ltv}, purchases=${customerData.purchases_count}`);

  // 4. Получаем ВСЕ транзакции (постранично), считаем 2-летнее окно
  const allTxns: Tx[] = [];
  let txPage = 1;
  while (true) {
    const r = await fetch(
      `${BASE_URL}/api/v4/customers/${customerId}/transactions?limit=250&page=${txPage}`,
      { headers }
    );
    if (r.status === 204 || !r.ok) break;
    const d = await r.json();
    for (const t of d?._embedded?.transactions || []) {
      allTxns.push({ id: t.id, price: t.price, completed_at: t.completed_at, comment: t.comment || '' });
    }
    if (!d._links?.next) break;
    txPage++;
  }

  if (allTxns.length === 0) {
    console.error('No transactions found');
    process.exit(1);
  }

  const stats = calcTwoYearStats(allTxns);
  console.log(`[${ts()}] 2y window: count=${stats.count} sum=${stats.sum} avg=${stats.avg} lastAt=${stats.lastAt}`);

  // Обновляем три кастомных поля у покупателя на лету (даже если 0/0/0)
  const fieldsPatchBody = [{
    id: customerId,
    custom_fields_values: buildTwoYearFieldsPatch(stats.sum, stats.count, stats.avg),
  }];
  const fpr = await fetch(`${BASE_URL}/api/v4/customers`, {
    method: 'PATCH',
    headers,
    body: JSON.stringify(fieldsPatchBody),
  });
  console.log(`[${ts()}] 2y fields patch: ${fpr.status}`);

  // 5. Если в окне 2 года нет покупок → Архив
  let segment: string;
  let daysSince = 0;
  let revPct = 0;
  let freqPct = 0;

  if (stats.count === 0) {
    segment = 'Архив';
    console.log(`[${ts()}] No purchases in 2y window → segment "Архив"`);
  } else {
    const cutoff = cutoffTimestamp();
    let firstAt = 0;
    for (const t of allTxns) {
      if (t.completed_at < cutoff) continue;
      if (firstAt === 0 || t.completed_at < firstAt) firstAt = t.completed_at;
    }

    const out = calculateSingleSegment(
      stats.sum,
      stats.count,
      stats.lastAt,
      thresholds.revenues,
      thresholds.frequencies,
      firstAt
    );
    segment = out.segment;
    daysSince = out.daysSince;
    revPct = out.revPct;
    freqPct = out.freqPct;
    console.log(`[${ts()}] Segment: ${segment} (rev:${Math.round(revPct * 100)}%, freq:${Math.round(freqPct * 100)}%, days:${daysSince}, daysSinceFirst:${out.daysSinceFirst})`);
  }

  // 6. PATCH покупателя
  const segmentId = SEGMENT_IDS[segment];
  const patchRes = await fetch(`${BASE_URL}/api/v4/customers`, {
    method: 'PATCH',
    headers,
    body: JSON.stringify([
      {
        id: customerId,
        _embedded: { segments: [{ id: segmentId }] },
      },
    ]),
  });

  if (patchRes.ok) {
    console.log(`[${ts()}] Customer #${customerId} → segment "${segment}" (${segmentId})`);
  } else {
    const text = await patchRes.text();
    console.error(`[${ts()}] Customer PATCH failed: ${patchRes.status} ${text}`);
  }

  // 7. PATCH контакта: поле "RFM-сегмент" + тег
  const contactsUpdated = await updateContactSegments([
    { contactId, segment },
  ]);
  console.log(`[${ts()}] Done. Contact #${contactId} → "${segment}" (updated: ${contactsUpdated})`);

}

main().catch((err) => {
  console.error('FAILED:', err);
  process.exit(1);
});
