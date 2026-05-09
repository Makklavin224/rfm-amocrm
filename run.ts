#!/usr/bin/env npx tsx
// Основной скрипт RFM-пересчёта. Запускается по cron или вебхуку.
// Источник данных: Покупатели (транзакции).
// Результат: обновление сегментов в Динамической сегментации.
import {
  fetchCustomersForRFM,
  updateCustomerSegments,
  updateContactSegments,
  SEGMENT_IDS,
} from './lib/amocrm';
import { calculateSegments, getThresholds } from './lib/rfm';
import { loadThresholds } from './lib/thresholds';

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log(`[${new Date().toISOString()}] RFM recalculation started`);

  // 1. Загружаем покупателей. Активные — с покупками за 2 года, архивные — без.
  console.log(`[${ts()}] Fetching customers (using 2y custom fields)...`);
  const { active: customers, archive } = await fetchCustomersForRFM();
  console.log(`[${ts()}] Active=${customers.length}, Archive=${archive.length}`);

  if (customers.length === 0 && archive.length === 0) {
    console.log('No customers found. Exiting.');
    return;
  }

  // 2. Рассчитываем сегменты с перцентилями (только по активным)
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
  if (archive.length > 0) {
    dist['Архив'] = (dist['Архив'] || 0) + archive.length;
  }
  console.log(`[${ts()}] Segments:`, dist);

  // 3. Обновляем сегменты покупателей (активные + архивные)
  const archiveSegmentId = SEGMENT_IDS['Архив'];
  const updates = [
    ...segments
      .map((s) => {
        const segmentId = SEGMENT_IDS[s.segment];
        if (!segmentId) {
          console.warn(`Unknown segment: "${s.segment}"`);
          return null;
        }
        return { customerId: s.customerId, segmentId };
      })
      .filter((u): u is NonNullable<typeof u> => u !== null),
    ...archive.map((a) => ({ customerId: a.customerId, segmentId: archiveSegmentId })),
  ];

  console.log(`[${ts()}] Updating ${updates.length} customer segments (active+archive)...`);
  const updated = await updateCustomerSegments(updates);

  console.log(`[${ts()}] Customers updated: ${updated}/${updates.length}`);

  // 4. Обновляем контакты: поле "RFM-сегмент" + тег
  const contactUpdates = [
    ...segments
      .filter((s) => s.contactId)
      .map((s) => ({ contactId: s.contactId!, segment: s.segment as string })),
    ...archive
      .filter((a) => a.contactId)
      .map((a) => ({ contactId: a.contactId!, segment: 'Архив' as string })),
  ];

  console.log(`[${ts()}] Updating ${contactUpdates.length} contacts (field + tag)...`);
  const contactsUpdated = await updateContactSegments(contactUpdates);

  console.log(`[${ts()}] Done. Contacts updated: ${contactsUpdated}`);
  console.log(`[${ts()}] Distribution:`, dist);
}

main().catch((err) => {
  console.error('RFM FAILED:', err);
  process.exit(1);
});
