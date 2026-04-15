// Дебаг: показывает состояние покупателя и какой сегмент должен быть
import { SEGMENT_IDS } from './lib/amocrm';

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function main() {
  const customerId = parseInt(process.argv[2]);
  if (!customerId) {
    console.error('Usage: npx tsx debug-customer.ts <customer_id>');
    process.exit(1);
  }

  console.log(`=== Debug customer #${customerId} ===\n`);

  // 1. Данные покупателя
  const custRes = await fetch(
    `${BASE_URL}/api/v4/customers/${customerId}?with=contacts`,
    { headers }
  );
  const cust = await custRes.json();

  console.log('Customer:');
  console.log(`  Name: ${cust.name}`);
  console.log(`  LTV: ${cust.ltv}`);
  console.log(`  Purchases: ${cust.purchases_count}`);
  console.log(`  Average check: ${cust.average_check}`);
  console.log(`  Contacts: ${(cust._embedded?.contacts || []).map((c: any) => c.id).join(', ')}`);
  console.log(`  Current segments: ${(cust._embedded?.segments || []).map((s: any) => s.id).join(', ')}`);

  // 2. Транзакции
  const txRes = await fetch(
    `${BASE_URL}/api/v4/customers/${customerId}/transactions?limit=50`,
    { headers }
  );
  const txData = await txRes.json();
  const txns = txData?._embedded?.transactions || [];

  console.log(`\nTransactions (${txns.length}):`);
  const sorted = [...txns].sort((a, b) => b.completed_at - a.completed_at);
  for (const tx of sorted.slice(0, 10)) {
    const d = new Date(tx.completed_at * 1000).toISOString().split('T')[0];
    console.log(`  [${d}] ${tx.price}₽ — ${(tx.comment || '').slice(0, 50)}`);
  }

  if (txns.length === 0) {
    console.log('  NO TRANSACTIONS — покупатель не попадёт в RFM расчёт');
    return;
  }

  // 3. Давность последней покупки
  const lastAt = Math.max(...txns.map((t: any) => t.completed_at));
  const daysSince = Math.floor((Date.now() / 1000 - lastAt) / 86400);
  console.log(`\nLast purchase: ${new Date(lastAt * 1000).toISOString().split('T')[0]} (${daysSince} days ago)`);

  // 4. Сравнение с базой
  console.log('\nCalculating percentiles vs entire base...');
  const all = await fetchAllCustomersWithPurchases();
  console.log(`  Base size: ${all.length} customers with purchases`);

  const revenues = all.map((c) => c.ltv).sort((a, b) => a - b);
  const freqs = all.map((c) => c.pcount).sort((a, b) => a - b);

  const revPct = revenues.filter((v) => v < cust.ltv).length / (revenues.length - 1);
  const freqPct = freqs.filter((v) => v < cust.purchases_count).length / (freqs.length - 1);

  console.log(`  Revenue ${cust.ltv}₽ → percentile ${(revPct * 100).toFixed(1)}%`);
  console.log(`  Frequency ${cust.purchases_count} → percentile ${(freqPct * 100).toFixed(1)}%`);

  // 5. Какой сегмент должен быть
  let expected = '';
  if (daysSince > 730) expected = 'Архив';
  else if (daysSince > 390) expected = revPct >= 0.9 ? 'VIP/КИТ в оттоке' : 'Потерянный';
  else if (revPct >= 0.9) expected = freqPct >= 0.5 ? 'VIP' : 'Киты';
  else if (revPct >= 0.5) expected = freqPct >= 0.5 ? 'Лояльные' : 'Перспективные';
  else if (daysSince <= 60) expected = 'Новичок';
  else expected = 'В зоне риска';

  console.log(`\n>>> EXPECTED segment: "${expected}" (id=${SEGMENT_IDS[expected]})`);
  console.log(`>>> CURRENT segments: ${(cust._embedded?.segments || []).map((s: any) => s.id).join(', ') || '(none)'}`);
}

async function fetchAllCustomersWithPurchases() {
  const result: Array<{ id: number; ltv: number; pcount: number }> = [];
  let page = 1;
  while (true) {
    const res = await fetch(
      `${BASE_URL}/api/v4/customers?limit=250&page=${page}`,
      { headers }
    );
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();
    for (const c of data?._embedded?.customers || []) {
      if (c.ltv && c.purchases_count) {
        result.push({ id: c.id, ltv: c.ltv, pcount: c.purchases_count });
      }
    }
    if (!data._links?.next) break;
    page++;
    await new Promise((r) => setTimeout(r, 200));
  }
  return result;
}

main().catch(console.error);
