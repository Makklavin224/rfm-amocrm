// Скользящее окно 730 дней: фильтр транзакций и агрегаты.
// Используется для расчёта "Сумма / Кол-во / Средний чек (последние 2 года)".

export const TWO_YEAR_DAYS = 730;

export interface Tx {
  id: number;
  price: number;
  completed_at: number; // unix timestamp (sec)
  comment?: string;
}

export interface TwoYearStats {
  count: number;
  sum: number;
  avg: number; // 0 если count=0
  lastAt: number; // 0 если count=0
}

export function cutoffTimestamp(now: Date = new Date()): number {
  const cutoffMs = now.getTime() - TWO_YEAR_DAYS * 24 * 60 * 60 * 1000;
  return Math.floor(cutoffMs / 1000);
}

export function calcTwoYearStats(txns: Tx[], now: Date = new Date()): TwoYearStats {
  const cutoff = cutoffTimestamp(now);
  let count = 0;
  let sum = 0;
  let lastAt = 0;
  for (const t of txns) {
    if (!t.completed_at || t.completed_at < cutoff) continue;
    count++;
    sum += t.price || 0;
    if (t.completed_at > lastAt) lastAt = t.completed_at;
  }
  const avg = count > 0 ? Math.round(sum / count) : 0;
  return { count, sum, avg, lastAt };
}
