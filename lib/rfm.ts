import type { CustomerData } from './amocrm';
import { saveThresholds, percentRank } from './thresholds';

// ─── Типы ───────────────────────────────────────────────

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

export interface CustomerSegment {
  customerId: number;
  segment: RfmSegment;
  daysSinceLastPurchase: number;
  purchaseCount: number;
  totalRevenue: number;
  revenuePercentile: number;
  frequencyPercentile: number;
}

// ─── PERCENTRANK (аналог Google Sheets ПРОЦЕНТРАНГ.ВКЛ) ─

function percentRankInc(values: number[], target: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  if (n <= 1) return 1;

  let countBelow = 0;
  for (const v of sorted) {
    if (v < target) countBelow++;
  }

  return countBelow / (n - 1);
}

// ─── Рассчитать сегменты из данных покупателей ──────────

export function calculateSegments(customers: CustomerData[]): CustomerSegment[] {
  const now = Date.now() / 1000;

  const withDays = customers.map((c) => ({
    ...c,
    daysSince: Math.floor((now - c.lastPurchaseAt) / 86400),
  }));

  const activeBase = withDays.filter((c) => c.daysSince <= 730);
  const revenues = activeBase.map((c) => c.ltv).sort((a, b) => a - b);
  const frequencies = activeBase.map((c) => c.purchasesCount).sort((a, b) => a - b);

  // Сохраняем пороги для быстрых одиночных пересчётов
  saveThresholds({
    updatedAt: new Date().toISOString(),
    baseSize: activeBase.length,
    revenues,
    frequencies,
  });

  return withDays.map((c) => {
    const revPct = percentRank(revenues, c.ltv);
    const freqPct = percentRank(frequencies, c.purchasesCount);

    const segment = assignSegment(c.daysSince, revPct, freqPct);

    return {
      customerId: c.customerId,
      segment,
      daysSinceLastPurchase: c.daysSince,
      purchaseCount: c.purchasesCount,
      totalRevenue: c.ltv,
      revenuePercentile: Math.round(revPct * 100),
      frequencyPercentile: Math.round(freqPct * 100),
    };
  });
}

// ─── Сегмент для одного покупателя (использует кэш) ──────

export function calculateSingleSegment(
  ltv: number,
  purchasesCount: number,
  lastPurchaseAt: number,
  revenues: number[],
  frequencies: number[]
): { segment: RfmSegment; daysSince: number; revPct: number; freqPct: number } {
  const now = Date.now() / 1000;
  const daysSince = Math.floor((now - lastPurchaseAt) / 86400);
  const revPct = percentRank(revenues, ltv);
  const freqPct = percentRank(frequencies, purchasesCount);
  const segment = assignSegment(daysSince, revPct, freqPct);
  return { segment, daysSince, revPct, freqPct };
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
  if (daysSince > 730) {
    return 'Архив';
  }

  if (daysSince > 390) {
    if (revPercentile >= 0.9) {
      return 'VIP/КИТ в оттоке';
    }
    return 'Потерянный';
  }

  if (revPercentile >= 0.9) {
    if (freqPercentile >= 0.5) {
      return 'VIP';
    }
    return 'Киты';
  }

  if (revPercentile >= 0.5) {
    if (freqPercentile >= 0.5) {
      return 'Лояльные';
    }
    return 'Перспективные';
  }

  if (daysSince <= 60) {
    return 'Новичок';
  }

  return 'В зоне риска';
}
