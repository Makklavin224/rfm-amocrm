// ID кастомных полей на сущности Покупатель (Customer) в amoCRM.

export const CUSTOMER_FIELD_IDS = {
  TWO_YEAR_SUM: 2304611,    // Сумма покупок (последние 2 года)
  TWO_YEAR_COUNT: 2304613,  // Кол-во покупок (последние 2 года)
  TWO_YEAR_AVG: 2304615,    // Средний чек (последние 2 года)
} as const;

interface RawField { field_id: number; values?: Array<{ value: any }>; }

export function readNumericField(customer: any, fieldId: number): number {
  const fields: RawField[] = customer?.custom_fields_values || [];
  const f = fields.find((x) => x.field_id === fieldId);
  if (!f || !f.values?.length) return 0;
  const v = f.values[0].value;
  if (typeof v === 'number') return v;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function buildTwoYearFieldsPatch(sum: number, count: number, avg: number) {
  return [
    { field_id: CUSTOMER_FIELD_IDS.TWO_YEAR_SUM, values: [{ value: sum }] },
    { field_id: CUSTOMER_FIELD_IDS.TWO_YEAR_COUNT, values: [{ value: count }] },
    { field_id: CUSTOMER_FIELD_IDS.TWO_YEAR_AVG, values: [{ value: avg }] },
  ];
}
