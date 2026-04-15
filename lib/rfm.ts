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

// ─── Дерево решений (ТЗ 2026-04) ────────────────────────
//
// Давность (Recency) — те же пороги:
//   > 730 дней → Архив
//   391-730 дней → зона оттока (VIP/КИТ в оттоке или Потерянный)
//   ≤ 390 дней → активная база
//   ≤ 60 дней → новые (Новичок)
//
// Пороги (вычисляются на активной базе ≤ 730 дней):
//   Выручка:    P50, P90
//   Частота:    P80, P90
//
// Сегменты:
//   VIP              revenue ≥ P90  AND purchases > P80
//   Киты             revenue ≥ P90  AND purchases ≤ P80
//   Лояльные         P50 ≤ revenue < P90  AND purchases > P90
//   Перспективные    P50 ≤ revenue < P90  AND purchases ≤ P90
//   Новичок          revenue < P50  AND days ≤ 60
//   В зоне риска     revenue < P50  AND days > 60
//   VIP/КИТ в оттоке days 391-730 AND revenue ≥ P90
//   Потерянный       days 391-730 AND revenue < P90
//   Архив            days > 730

function assignSegment(
  daysSince: number,
  revPercentile: number,
  freqPercentile: number
): RfmSegment {
  if (daysSince > 730) return 'Архив';

  if (daysSince > 390) {
    return revPercentile >= 0.9 ? 'VIP/КИТ в оттоке' : 'Потерянный';
  }

  // Активная база (≤ 390 дней)
  if (revPercentile >= 0.9) {
    // Топ-10% по деньгам
    return freqPercentile > 0.8 ? 'VIP' : 'Киты';
  }

  if (revPercentile >= 0.5) {
    // Средний класс (P50-P90 по деньгам)
    return freqPercentile > 0.9 ? 'Лояльные' : 'Перспективные';
  }

  // Нижняя половина по деньгам
  return daysSince <= 60 ? 'Новичок' : 'В зоне риска';
}

// ─── Пороговые значения (для логов/отображения) ─────────

export interface ThresholdValues {
  revenueP50: number;
  revenueP90: number;
  frequencyP80: number;
  frequencyP90: number;
}

export function getThresholds(
  revenues: number[],
  frequencies: number[]
): ThresholdValues {
  // Google Sheets PERCENTILE: линейная интерполяция
  const pct = (sorted: number[], p: number): number => {
    const n = sorted.length;
    if (n === 0) return 0;
    if (n === 1) return sorted[0];
    const idx = p * (n - 1);
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    const frac = idx - lo;
    return sorted[lo] * (1 - frac) + sorted[hi] * frac;
  };

  return {
    revenueP50: Math.round(pct(revenues, 0.5)),
    revenueP90: Math.round(pct(revenues, 0.9)),
    frequencyP80: Math.round(pct(frequencies, 0.8) * 100) / 100,
    frequencyP90: Math.round(pct(frequencies, 0.9) * 100) / 100,
  };
}
