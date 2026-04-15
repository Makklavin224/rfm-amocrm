// Показывает текущие пороги RFM из .thresholds.json
// Запуск: npx tsx show-thresholds.ts
import { loadThresholds } from './lib/thresholds';
import { getThresholds } from './lib/rfm';

function fmt(n: number): string {
  return n.toLocaleString('ru-RU') + ' ₽';
}

function main() {
  const cache = loadThresholds();
  if (!cache) {
    console.error('Кэш порогов не найден. Запусти `npx tsx run.ts` сначала.');
    process.exit(1);
  }

  const t = getThresholds(cache.revenues, cache.frequencies);
  const updatedAt = new Date(cache.updatedAt).toLocaleString('ru-RU');

  console.log('═'.repeat(70));
  console.log(`  RFM пороги (обновлено: ${updatedAt})`);
  console.log(`  Активная база: ${cache.baseSize} покупателей (за последние 730 дней)`);
  console.log('═'.repeat(70));

  console.log('\n📊 ПЕРЦЕНТИЛИ:');
  console.log(`  Выручка:`);
  console.log(`    P50 (медиана)  = ${fmt(t.revenueP50)}`);
  console.log(`    P90 (топ-10%)  = ${fmt(t.revenueP90)}`);
  console.log(`  Частота покупок:`);
  console.log(`    P80 (топ-20%)  = ${t.frequencyP80}`);
  console.log(`    P90 (топ-10%)  = ${t.frequencyP90}`);

  console.log('\n📋 КРИТЕРИИ СЕГМЕНТОВ:\n');

  const segments = [
    {
      name: '🟡 VIP',
      days: 'до 390 дней',
      revenue: `от ${fmt(t.revenueP90)}`,
      freq: `от ${t.frequencyP80}`,
      action: 'VIP сервис, персональный менеджер',
    },
    {
      name: '🔴 Киты',
      days: 'до 390 дней',
      revenue: `от ${fmt(t.revenueP90)}`,
      freq: `менее ${t.frequencyP80}`,
      action: 'Cross-sell, программы лояльности',
    },
    {
      name: '🟢 Лояльные',
      days: 'до 390 дней',
      revenue: `${fmt(t.revenueP50)} — ${fmt(t.revenueP90)}`,
      freq: `от ${t.frequencyP80}`,
      action: 'Up-sell, пакетные предложения',
    },
    {
      name: '🟢 Перспективные',
      days: 'до 390 дней',
      revenue: `${fmt(t.revenueP50)} — ${fmt(t.revenueP90)}`,
      freq: `менее ${t.frequencyP80}`,
      action: 'Обучающий контент, экспертность',
    },
    {
      name: '🟢 Новичок',
      days: 'до 60 дней',
      revenue: `до ${fmt(t.revenueP50)}`,
      freq: 'любая',
      action: 'Welcome-цепочка, конверсия во 2-ю покупку',
    },
    {
      name: '🟡 В зоне риска',
      days: '60 — 390 дней',
      revenue: `до ${fmt(t.revenueP50)}`,
      freq: 'любая',
      action: 'Реактивация, опрос «почему пропали»',
    },
    {
      name: '🔴 VIP/КИТ в оттоке',
      days: '391 — 730 дней',
      revenue: `от ${fmt(t.revenueP90)}`,
      freq: 'любая',
      action: 'Звонок РОПа, эксклюзивные условия',
    },
    {
      name: '⚪ Потерянный',
      days: '391 — 730 дней',
      revenue: `до ${fmt(t.revenueP90)}`,
      freq: 'любая',
      action: 'Массовая рассылка с супер-оффером',
    },
    {
      name: '⚫ Архив',
      days: 'более 730 дней',
      revenue: 'любая',
      freq: 'любая',
      action: 'Удалить из активных рассылок',
    },
  ];

  for (const s of segments) {
    console.log(`${s.name}`);
    console.log(`  Давность:  ${s.days}`);
    console.log(`  Выручка:   ${s.revenue}`);
    console.log(`  Покупок:   ${s.freq}`);
    console.log(`  Действие:  ${s.action}`);
    console.log('');
  }

  console.log('═'.repeat(70));
}

main();
