import type { AmoDeal } from './amocrm';

// ─── Типы ───────────────────────────────────────────────

export interface ContactRFM {
  contactId: number;
  lastPurchaseDate: Date;
  daysSinceLastPurchase: number;
  purchaseCount: number;
  totalRevenue: number;
}

export type RfmSegment =
  | 'VIP'
  | 'Киты'
  | 'Лояльные'
  | 'Перспективные'
  | 'Новичок'
  | 'В зоне риска'
  | 'VIP/КИТ в оттоке'
  | 'Потерянный'
  | 'Архив';

export interface ContactSegment {
  contactId: number;
  segment: RfmSegment;
  daysSinceLastPurchase: number;
  purchaseCount: number;
  totalRevenue: number;
  revenuePercentile: number;
  frequencyPercentile: number;
}

// ─── 1. Агрегация сделок по контакту ────────────────────

export function aggregateByContact(deals: AmoDeal[]): ContactRFM[] {
  const map = new Map<
    number,
    { lastDate: number; count: number; revenue: number }
  >();

  for (const deal of deals) {
    if (!deal.contact_id || !deal.closed_at) continue;

    const existing = map.get(deal.contact_id);
    if (existing) {
      existing.lastDate = Math.max(existing.lastDate, deal.closed_at);
      existing.count++;
      existing.revenue += deal.price;
    } else {
      map.set(deal.contact_id, {
        lastDate: deal.closed_at,
        count: 1,
        revenue: deal.price,
      });
    }
  }

  const now = new Date();
  const results: ContactRFM[] = [];

  for (const [contactId, data] of map) {
    const lastPurchaseDate = new Date(data.lastDate * 1000);
    const daysSince = Math.floor(
      (now.getTime() - lastPurchaseDate.getTime()) / (1000 * 60 * 60 * 24)
    );

    results.push({
      contactId,
      lastPurchaseDate,
      daysSinceLastPurchase: daysSince,
      purchaseCount: data.count,
      totalRevenue: data.revenue,
    });
  }

  return results;
}

// ─── 2. PERCENTRANK (аналог Google Sheets ПРОЦЕНТРАНГ.ВКЛ) ─

function percentRankInc(values: number[], target: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  if (n <= 1) return 1;

  let countBelow = 0;
  let countEqual = 0;
  for (const v of sorted) {
    if (v < target) countBelow++;
    else if (v === target) countEqual++;
  }

  // PERCENTRANK.INC формула: (rank - 1) / (n - 1)
  // rank = countBelow + 1 (для первого вхождения)
  return countBelow / (n - 1);
}

// ─── 3. Рассчитать сегменты (дерево решений из документа) ─

export function calculateSegments(contacts: ContactRFM[]): ContactSegment[] {
  // Фильтруем активную базу (до 730 дней) для расчёта перцентилей
  const activeBase = contacts.filter((c) => c.daysSinceLastPurchase <= 730);

  // Массивы значений для перцентилей
  const revenues = activeBase.map((c) => c.totalRevenue);
  const frequencies = activeBase.map((c) => c.purchaseCount);

  return contacts.map((c) => {
    // Считаем перцентили только по активной базе
    const revPct = percentRankInc(revenues, c.totalRevenue);
    const freqPct = percentRankInc(frequencies, c.purchaseCount);

    const segment = assignSegment(
      c.daysSinceLastPurchase,
      revPct,
      freqPct
    );

    return {
      contactId: c.contactId,
      segment,
      daysSinceLastPurchase: c.daysSinceLastPurchase,
      purchaseCount: c.purchaseCount,
      totalRevenue: c.totalRevenue,
      revenuePercentile: Math.round(revPct * 100),
      frequencyPercentile: Math.round(freqPct * 100),
    };
  });
}

// ─── Дерево решений из документа ────────────────────────
//
// Этап 1: > 730 дней → Архив
// Этап 2: 391-730 дней → проверяем деньги
//   - Топ-10% по выручке → VIP/КИТ в оттоке
//   - Остальные → Потерянный
// Этап 3: ≤ 390 дней → полный анализ
//   - Топ-10% по выручке (≥ 0.9):
//     - Частота ≥ 0.5 → VIP
//     - Частота < 0.5 → Киты
//   - Топ-50% по выручке (≥ 0.5):
//     - Частота ≥ 0.5 → Лояльные
//     - Частота < 0.5 → Перспективные
//   - Нижние 50%:
//     - ≤ 60 дней → Новичок
//     - > 60 дней → В зоне риска

function assignSegment(
  daysSince: number,
  revPercentile: number,
  freqPercentile: number
): RfmSegment {
  // Этап 1: Архив
  if (daysSince > 730) {
    return 'Архив';
  }

  // Этап 2: Зона оттока (391-730 дней)
  if (daysSince > 390) {
    if (revPercentile >= 0.9) {
      return 'VIP/КИТ в оттоке';
    }
    return 'Потерянный';
  }

  // Этап 3: Активная база (≤ 390 дней)

  // Блок «Элита» — топ-10% по выручке
  if (revPercentile >= 0.9) {
    if (freqPercentile >= 0.5) {
      return 'VIP';
    }
    return 'Киты';
  }

  // Блок «Средний класс» — топ-50% по выручке
  if (revPercentile >= 0.5) {
    if (freqPercentile >= 0.5) {
      return 'Лояльные';
    }
    return 'Перспективные';
  }

  // Хвост базы
  if (daysSince <= 60) {
    return 'Новичок';
  }

  return 'В зоне риска';
}
