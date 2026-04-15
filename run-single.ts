#!/usr/bin/env npx tsx
// Пересчёт сегмента для ОДНОГО покупателя по ID сделки.
// Использует кэшированные перцентили из .thresholds.json.
// Usage: npx tsx run-single.ts <lead_id>

import { SEGMENT_IDS } from './lib/amocrm';
import { calculateSingleSegment } from './lib/rfm';
import { loadThresholds } from './lib/thresholds';

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

  console.log(`[${ts()}] Customer #${customerId}: ltv=${customerData.ltv}, purchases=${customerData.purchases_count}`);

  if (!customerData.ltv || !customerData.purchases_count) {
    console.error('Customer has no purchases yet');
    process.exit(1);
  }

  // 4. Получаем дату последней транзакции
  const txRes = await fetch(
    `${BASE_URL}/api/v4/customers/${customerId}/transactions?limit=250`,
    { headers }
  );
  const txData = await txRes.json();
  const txns = txData?._embedded?.transactions || [];

  if (txns.length === 0) {
    console.error('No transactions found');
    process.exit(1);
  }

  const lastAt = Math.max(...txns.map((t: any) => t.completed_at));

  // 5. Считаем сегмент по кэшированным порогам
  const { segment, daysSince, revPct, freqPct } = calculateSingleSegment(
    customerData.ltv,
    customerData.purchases_count,
    lastAt,
    thresholds.revenues,
    thresholds.frequencies
  );

  console.log(`[${ts()}] Segment: ${segment} (rev:${Math.round(revPct * 100)}%, freq:${Math.round(freqPct * 100)}%, days:${daysSince})`);

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
    console.log(`[${ts()}] Done. Customer #${customerId} → segment "${segment}" (${segmentId})`);
  } else {
    const text = await patchRes.text();
    console.error(`[${ts()}] PATCH failed: ${patchRes.status} ${text}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('FAILED:', err);
  process.exit(1);
});
