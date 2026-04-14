#!/usr/bin/env npx tsx
// Основной скрипт RFM-пересчёта. Запускается по cron.
// Записывает результат в Покупатели → Динамическая сегментация.
import {
  fetchAllWonDeals,
  fetchAllCustomers,
  createCustomers,
  updateCustomerSegments,
  SEGMENT_IDS,
} from './lib/amocrm';
import { aggregateByContact, calculateSegments } from './lib/rfm';

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${new Date().toISOString()}] RFM recalculation started`);

  // 1. Загружаем выигранные сделки
  console.log(`[${ts()}] Fetching won deals...`);
  const deals = await fetchAllWonDeals();
  console.log(`[${ts()}] Loaded ${deals.length} won deals`);

  if (deals.length === 0) {
    console.log('No won deals found. Exiting.');
    return;
  }

  // 2. Агрегируем по контакту: R, F, M
  const contactData = aggregateByContact(deals);
  console.log(`[${ts()}] ${contactData.length} unique contacts`);

  // 3. Рассчитываем сегменты с перцентилями
  const segments = calculateSegments(contactData);

  const dist: Record<string, number> = {};
  for (const s of segments) {
    dist[s.segment] = (dist[s.segment] || 0) + 1;
  }
  console.log(`[${ts()}] Segments:`, dist);

  // 4. Загружаем существующих покупателей → map contact_id → customer_id
  console.log(`[${ts()}] Fetching existing customers...`);
  const contactToCustomer = await fetchAllCustomers();
  console.log(`[${ts()}] Found ${contactToCustomer.size} existing customers`);

  // 5. Разделяем: у кого есть покупатель, у кого нет
  const segmentMap = new Map(segments.map((s) => [s.contactId, s.segment]));
  const withCustomer: Array<{ customerId: number; segment: string }> = [];
  const withoutCustomer: number[] = [];

  for (const s of segments) {
    const custId = contactToCustomer.get(s.contactId);
    if (custId) {
      withCustomer.push({ customerId: custId, segment: s.segment });
    } else {
      withoutCustomer.push(s.contactId);
    }
  }

  console.log(`[${ts()}] With customer: ${withCustomer.length}, need to create: ${withoutCustomer.length}`);

  // 6. Создаём покупателей для контактов без них
  if (withoutCustomer.length > 0) {
    console.log(`[${ts()}] Creating ${withoutCustomer.length} new customers...`);
    const created = await createCustomers(withoutCustomer, segmentMap);
    console.log(`[${ts()}] Created ${created.size} customers`);

    // Добавляем в список на обновление (у новых сегмент уже задан при создании)
    // Но на всякий случай обновим и их тоже
    for (const [contactId, customerId] of created) {
      const segment = segmentMap.get(contactId);
      if (segment) {
        withCustomer.push({ customerId, segment });
      }
    }
  }

  // 7. Обновляем сегменты у всех покупателей
  const updates = withCustomer
    .map((u) => {
      const segmentId = SEGMENT_IDS[u.segment];
      if (!segmentId) {
        console.warn(`Unknown segment: "${u.segment}"`);
        return null;
      }
      return { customerId: u.customerId, segmentId };
    })
    .filter((u): u is NonNullable<typeof u> => u !== null);

  console.log(`[${ts()}] Updating ${updates.length} customer segments...`);
  const updated = await updateCustomerSegments(updates);

  console.log(`[${ts()}] Done. Updated: ${updated}/${segments.length}`);
  console.log(`[${ts()}] Distribution:`, dist);
}

main().catch((err) => {
  console.error('RFM FAILED:', err);
  process.exit(1);
});
