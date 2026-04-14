const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

const CONCURRENT = 5;

// ─── Типы ───────────────────────────────────────────────

export interface CustomerData {
  customerId: number;
  ltv: number;
  purchasesCount: number;
  lastPurchaseAt: number; // unix timestamp
}

// Маппинг сегмент → ID сегмента в amoCRM (Покупатели → Динамическая сегментация)
export const SEGMENT_IDS: Record<string, number> = {
  'VIP': 113,
  'VIP/КИТ в оттоке': 115,
  'Киты': 117,
  'Лояльные': 119,
  'Перспективные': 121,
  'Новичок': 123,
  'В зоне риска': 125,
  'Потерянный': 127,
  'Архив': 129,
};

// ─── Покупатели: загрузить всех с покупками ─────────────

export async function fetchCustomersWithPurchases(): Promise<CustomerData[]> {
  const customers: Array<{ id: number; ltv: number; purchasesCount: number }> = [];
  let page = 1;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers?limit=250&page=${page}`;
    const res = await fetch(url, { headers });

    if (res.status === 204) break;
    if (!res.ok) break;

    const data = await res.json();
    const items = data?._embedded?.customers || [];

    for (const cust of items) {
      if (cust.purchases_count && cust.purchases_count > 0 && cust.ltv) {
        customers.push({
          id: cust.id,
          ltv: cust.ltv,
          purchasesCount: cust.purchases_count,
        });
      }
    }

    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }

  console.log(`  Fetched ${customers.length} customers with purchases`);

  // Для каждого покупателя получаем дату последней транзакции
  const result: CustomerData[] = [];
  const chunks = chunkArray(customers, CONCURRENT);

  for (const chunk of chunks) {
    const promises = chunk.map(async (cust) => {
      const url = `${BASE_URL}/api/v4/customers/${cust.id}/transactions?limit=1`;
      const res = await fetch(url, { headers });

      if (!res.ok || res.status === 204) return null;

      const data = await res.json();
      const txns = data?._embedded?.transactions || [];

      if (txns.length === 0) return null;

      // Берём самую свежую транзакцию (API возвращает desc по умолчанию)
      const lastTx = txns.reduce(
        (max: any, tx: any) => (tx.completed_at > max.completed_at ? tx : max),
        txns[0]
      );

      return {
        customerId: cust.id,
        ltv: cust.ltv,
        purchasesCount: cust.purchasesCount,
        lastPurchaseAt: lastTx.completed_at,
      };
    });

    const results = await Promise.all(promises);
    for (const r of results) {
      if (r) result.push(r);
    }

    await sleep(200);
  }

  return result;
}

// ─── Покупатели: обновить сегменты ──────────────────────

export interface CustomerSegmentUpdate {
  customerId: number;
  segmentId: number;
}

export async function updateCustomerSegments(
  updates: CustomerSegmentUpdate[]
): Promise<number> {
  let updated = 0;
  const chunks = chunkArray(updates, 50);

  for (const chunk of chunks) {
    const body = chunk.map((u) => ({
      id: u.customerId,
      _embedded: {
        segments: [{ id: u.segmentId }],
      },
    }));

    const res = await fetch(`${BASE_URL}/api/v4/customers`, {
      method: 'PATCH',
      headers,
      body: JSON.stringify(body),
    });

    if (res.ok) {
      updated += chunk.length;
    } else {
      const text = await res.text();
      console.error(`Update segments error: ${res.status} ${text}`);
    }

    await sleep(300);
  }

  return updated;
}

// ─── Утилиты ────────────────────────────────────────────

function chunkArray<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}
