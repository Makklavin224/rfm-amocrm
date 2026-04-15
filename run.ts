#!/usr/bin/env npx tsx
// Основной скрипт RFM-пересчёта. Запускается по cron или вебхуку.
// Источник данных: Покупатели (транзакции).
// Результат: обновление сегментов в Динамической сегментации.
import {
  fetchCustomersWithPurchases,
  updateCustomerSegments,
  SEGMENT_IDS,
} from './lib/amocrm';
import { calculateSegments, getThresholds } from './lib/rfm';
import { loadThresholds } from './lib/thresholds';

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${new Date().toISOString()}] RFM recalculation started`);

  // 1. Загружаем покупателей с транзакциями
  console.log(`[${ts()}] Fetching customers with purchases...`);
  const customers = await fetchCustomersWithPurchases();
  console.log(`[${ts()}] Found ${customers.length} customers with purchases`);

  if (customers.length === 0) {
    console.log('No customers with purchases found. Exiting.');
    return;
  }

  // 2. Рассчитываем сегменты с перцентилями
  const segments = calculateSegments(customers);

  // Показываем пороги
  const cache = loadThresholds();
  if (cache) {
    const t = getThresholds(cache.revenues, cache.frequencies);
    console.log(`[${ts()}] Пороги (активная база ${cache.baseSize}):`);
    console.log(`  Выручка P50 = ${t.revenueP50.toLocaleString('ru')}₽, P90 = ${t.revenueP90.toLocaleString('ru')}₽`);
    console.log(`  Частота P80 = ${t.frequencyP80}, P90 = ${t.frequencyP90}`);
  }

  const dist: Record<string, number> = {};
  for (const s of segments) {
    dist[s.segment] = (dist[s.segment] || 0) + 1;
  }
  console.log(`[${ts()}] Segments:`, dist);

  // 3. Обновляем сегменты покупателей
  const updates = segments
    .map((s) => {
      const segmentId = SEGMENT_IDS[s.segment];
      if (!segmentId) {
        console.warn(`Unknown segment: "${s.segment}"`);
        return null;
      }
      return { customerId: s.customerId, segmentId };
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
