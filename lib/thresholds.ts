// Кэш перцентилей для быстрого пересчёта одного покупателя
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';

const FILE = join(__dirname, '..', '.thresholds.json');

export interface Thresholds {
  updatedAt: string;
  baseSize: number;
  revenues: number[]; // отсортированные значения ltv активной базы
  frequencies: number[]; // отсортированные значения purchases_count
}

export function saveThresholds(t: Thresholds): void {
  writeFileSync(FILE, JSON.stringify(t));
}

export function loadThresholds(): Thresholds | null {
  if (!existsSync(FILE)) return null;
  try {
    return JSON.parse(readFileSync(FILE, 'utf-8')) as Thresholds;
  } catch {
    return null;
  }
}

// Binary search: сколько значений < target
export function percentRank(sortedValues: number[], target: number): number {
  const n = sortedValues.length;
  if (n <= 1) return 1;

  let lo = 0,
    hi = n;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (sortedValues[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo / (n - 1);
}
