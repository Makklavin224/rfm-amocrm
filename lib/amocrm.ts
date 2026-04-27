const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

const CONCURRENT = 2;

// Обёртка fetch с retry на 429/403 и сетевые ошибки
async function fetchWithRetry(
  url: string,
  init?: RequestInit,
  maxRetries = 6
): Promise<Response> {
  let lastError: any = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await fetch(url, init);
      if (res.status !== 429 && res.status !== 403) return res;
      const wait = 2000 * Math.pow(2, attempt);
      console.warn(`  ${res.status} received, waiting ${wait}ms (attempt ${attempt + 1}/${maxRetries})`);
      await sleep(wait);
    } catch (err: any) {
      // Сетевые ошибки (socket closed, timeout, DNS)
      lastError = err;
      const wait = 2000 * Math.pow(2, attempt);
      console.warn(`  Network error (${err.cause?.code || err.code || 'unknown'}), waiting ${wait}ms (attempt ${attempt + 1}/${maxRetries})`);
      await sleep(wait);
    }
  }

  if (lastError) throw lastError;
  return fetch(url, init);
}

// ─── Типы ───────────────────────────────────────────────

export interface CustomerData {
  customerId: number;
  contactId: number | null;
  ltv: number;
  purchasesCount: number;
  lastPurchaseAt: number; // unix timestamp
}

// ID поля "RFM-сегмент" на контактах (создано ранее)
const RFM_FIELD_ID = 2304505;
const RFM_ENUM_IDS: Record<string, number> = {
  'VIP': 9438933,
  'Киты': 9438935,
  'Лояльные': 9438937,
  'Перспективные': 9438939,
  'Новичок': 9438941,
  'В зоне риска': 9438943,
  'VIP/КИТ в оттоке': 9438945,
  'Потерянный': 9438947,
  'Архив': 9438949,
};

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
  const customers: Array<{ id: number; contactId: number | null; ltv: number; purchasesCount: number }> = [];
  let page = 1;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`;
    const res = await fetchWithRetry(url, { headers });

    if (res.status === 204) break;
    if (!res.ok) {
      const text = await res.text();
      console.error(`  Customers fetch error page=${page}: ${res.status} ${text.slice(0, 200)}`);
      break;
    }

    const data = await res.json();
    const items = data?._embedded?.customers || [];

    for (const cust of items) {
      if (cust.purchases_count && cust.purchases_count > 0 && cust.ltv) {
        const mainContact = cust._embedded?.contacts?.[0];
        customers.push({
          id: cust.id,
          contactId: mainContact?.id || null,
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
      try {
        const url = `${BASE_URL}/api/v4/customers/${cust.id}/transactions?limit=1`;
        const res = await fetchWithRetry(url, { headers });

        if (!res.ok || res.status === 204) return null;

        const data = await res.json();
        const txns = data?._embedded?.transactions || [];

        if (txns.length === 0) return null;

        const lastTx = txns.reduce(
          (max: any, tx: any) => (tx.completed_at > max.completed_at ? tx : max),
          txns[0]
        );

        return {
          customerId: cust.id,
          contactId: cust.contactId,
          ltv: cust.ltv,
          purchasesCount: cust.purchasesCount,
          lastPurchaseAt: lastTx.completed_at,
        };
      } catch (err: any) {
        console.warn(`  Skip customer ${cust.id}: ${err.message}`);
        return null;
      }
    });

    const results = await Promise.all(promises);
    for (const r of results) {
      if (r) result.push(r);
    }

    await sleep(300);
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

    const res = await fetchWithRetry(`${BASE_URL}/api/v4/customers`, {
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

// ─── Контакты: обновить поле "RFM-сегмент" + тег ────────

export interface ContactSegmentUpdate {
  contactId: number;
  segment: string;
}

export async function updateContactSegments(
  updates: ContactSegmentUpdate[]
): Promise<number> {
  let updated = 0;
  const chunks = chunkArray(updates, 50);

  for (const chunk of chunks) {
    const body = chunk.map((u) => {
      const enumId = RFM_ENUM_IDS[u.segment];
      const tagName = `rfm:${u.segment.toLowerCase().replace(/[\s\/]/g, '-')}`;

      const patch: any = {
        id: u.contactId,
        _embedded: {
          tags: [{ name: tagName }],
        },
      };

      if (enumId) {
        patch.custom_fields_values = [
          {
            field_id: RFM_FIELD_ID,
            values: [{ enum_id: enumId }],
          },
        ];
      }

      return patch;
    });

    const res = await fetchWithRetry(`${BASE_URL}/api/v4/contacts`, {
      method: 'PATCH',
      headers,
      body: JSON.stringify(body),
    });

    if (res.ok) {
      updated += chunk.length;
    } else {
      const text = await res.text();
      console.error(`Update contacts error: ${res.status} ${text.slice(0, 200)}`);
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
