#!/usr/bin/env npx tsx
// Основной скрипт RFM-пересчёта. Запускается по cron.
import {
  fetchAllWonDeals,
  ensureRfmField,
  getFieldEnums,
  batchUpdateContacts,
  removeOldRfmTags,
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

  // 2. Агрегируем по контакту
  const contactData = aggregateByContact(deals);
  console.log(`[${ts()}] ${contactData.length} unique contacts`);

  // 3. Рассчитываем сегменты
  const segments = calculateSegments(contactData);

  const dist: Record<string, number> = {};
  for (const s of segments) {
    dist[s.segment] = (dist[s.segment] || 0) + 1;
  }
  console.log(`[${ts()}] Segments:`, dist);

  // 4. Создаём/находим поле
  const fieldId = await ensureRfmField();
  console.log(`[${ts()}] Field ID: ${fieldId}`);

  // 5. Получаем enum_id
  const enums = await getFieldEnums(fieldId);

  // 6. Удаляем старые rfm: теги
  const contactIds = segments.map((s) => s.contactId);
  console.log(`[${ts()}] Removing old tags...`);
  await removeOldRfmTags(contactIds);

  // 7. Обновляем контакты
  const updates = segments
    .map((s) => {
      const enumId = enums.get(s.segment);
      if (!enumId) return null;
      return { contactId: s.contactId, segment: s.segment, fieldId, enumId };
    })
    .filter((u): u is NonNullable<typeof u> => u !== null);

  console.log(`[${ts()}] Updating ${updates.length} contacts...`);
  const updated = await batchUpdateContacts(updates);

  console.log(`[${ts()}] Done. Updated: ${updated}/${segments.length}`);
  console.log(`[${ts()}] Distribution:`, dist);
}

main().catch((err) => {
  console.error('RFM FAILED:', err);
  process.exit(1);
});
