import { CUSTOMER_FIELD_IDS, readNumericField } from './customer-fields';
import { cutoffTimestamp } from './two-year-stats';

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
  lastPurchaseAt: number; // unix timestamp, max(completed_at) в окне 730 дней
  firstPurchaseAtIn2y: number; // unix timestamp, min(completed_at) в окне 730 дней
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

// ─── Покупатели: загрузить активных и архивных по 2-летнему окну ───
//
// Источник цифр: кастомные поля "Сумма / Кол-во (последние 2 года)".
// Поля заполняются скриптом recalc-2y-fields.ts (запускается ежедневно).
//
// active  — count_2y > 0  (попадают в RFM-расчёт)
// archive — count_2y == 0 и есть исторические покупки (purchases_count > 0)
//           → им проставляется сегмент "Архив"

export interface CustomersForRFM {
  active: CustomerData[];
  archive: Array<{ customerId: number; contactId: number | null }>;
}

export async function fetchCustomersForRFM(): Promise<CustomersForRFM> {
  const active: Array<{ id: number; contactId: number | null; ltv: number; purchasesCount: number }> = [];
  const archive: Array<{ customerId: number; contactId: number | null }> = [];
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
      const mainContact = cust._embedded?.contacts?.find((c: any) => c.is_main) || cust._embedded?.contacts?.[0];
      const contactId = mainContact?.id || null;

      const sum2y = readNumericField(cust, CUSTOMER_FIELD_IDS.TWO_YEAR_SUM);
      const count2y = readNumericField(cust, CUSTOMER_FIELD_IDS.TWO_YEAR_COUNT);
      const totalPurchases = cust.purchases_count || 0;

      if (count2y > 0 && sum2y > 0) {
        active.push({ id: cust.id, contactId, ltv: sum2y, purchasesCount: count2y });
      } else if (totalPurchases > 0) {
        // Был покупателем когда-то, но не за последние 730 дней → Архив
        archive.push({ customerId: cust.id, contactId });
      }
      // count_2y=0 И total=0 → пустая запись (например очищенный дубль), пропускаем
    }

    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }

  console.log(`  Active (in 2y window): ${active.length}, Archive (no 2y purchases): ${archive.length}`);

  // Для активных получаем дату последней транзакции В ОКНЕ
  const cutoff = cutoffTimestamp();
  const result: CustomerData[] = [];
  const chunks = chunkArray(active, CONCURRENT);

  for (const chunk of chunks) {
    const promises = chunk.map(async (cust) => {
      try {
        let maxCompletedAt = 0;
        let minCompletedAt = 0;
        let txPage = 1;

        while (true) {
          const url = `${BASE_URL}/api/v4/customers/${cust.id}/transactions?limit=250&page=${txPage}`;
          const res = await fetchWithRetry(url, { headers });
          if (!res.ok || res.status === 204) break;
          const data = await res.json();
          const txns = data?._embedded?.transactions || [];
          for (const tx of txns) {
            if (tx.completed_at < cutoff) continue;
            if (tx.completed_at > maxCompletedAt) maxCompletedAt = tx.completed_at;
            if (minCompletedAt === 0 || tx.completed_at < minCompletedAt) minCompletedAt = tx.completed_at;
          }
          if (!data._links?.next) break;
          txPage++;
          await sleep(150);
        }

        if (maxCompletedAt === 0) return null; // несогласованность 2y-полей и транзакций

        return {
          customerId: cust.id,
          contactId: cust.contactId,
          ltv: cust.ltv,
          purchasesCount: cust.purchasesCount,
          lastPurchaseAt: maxCompletedAt,
          firstPurchaseAtIn2y: minCompletedAt,
        };
      } catch (err: any) {
        console.warn(`  Skip customer ${cust.id}: ${err.message}`);
        return null;
      }
    });

    const results = await Promise.all(promises);
    for (const r of results) if (r) result.push(r);
    await sleep(300);
  }

  return { active: result, archive };
}

// Совместимость со старыми вызовами
export async function fetchCustomersWithPurchases(): Promise<CustomerData[]> {
  const { active } = await fetchCustomersForRFM();
  return active;
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

// Все возможные rfm: теги (для очистки старых)
const ALL_RFM_TAGS = [
  'rfm:vip',
  'rfm:киты',
  'rfm:лояльные',
  'rfm:перспективные',
  'rfm:новичок',
  'rfm:в-зоне-риска',
  'rfm:vip-кит-в-оттоке',
  'rfm:потерянный',
  'rfm:архив',
];

// Получить текущие теги для списка контактов (одним запросом по filter[id][])
async function fetchContactTags(
  contactIds: number[]
): Promise<Map<number, Array<{ id: number; name: string }>>> {
  const out = new Map<number, Array<{ id: number; name: string }>>();
  for (const id of contactIds) out.set(id, []);

  const q = contactIds.map((id) => `filter[id][]=${id}`).join('&');
  const url = `${BASE_URL}/api/v4/contacts?${q}&limit=250&with=tags`;
  const res = await fetchWithRetry(url, { headers });
  if (!res.ok) return out;
  const data = await res.json();
  for (const c of data?._embedded?.contacts || []) {
    const tags = (c._embedded?.tags || []).map((t: any) => ({ id: t.id, name: t.name }));
    out.set(c.id, tags);
  }
  return out;
}

export async function updateContactSegments(
  updates: ContactSegmentUpdate[]
): Promise<number> {
  let updated = 0;
  const chunks = chunkArray(updates, 50);

  for (const chunk of chunks) {
    // 1. Подтягиваем существующие теги контактов в этом батче
    const tagMap = await fetchContactTags(chunk.map((u) => u.contactId));

    // 2. Строим PATCH-тело: для каждого контакта = (существующие теги без rfm:*) + новый rfm-тег
    const body = chunk.map((u) => {
      const enumId = RFM_ENUM_IDS[u.segment];
      const newTag = `rfm:${u.segment.toLowerCase().replace(/[\s\/]/g, '-')}`;

      const existing = tagMap.get(u.contactId) || [];
      const keptTags = existing
        .filter((t) => !t.name.startsWith('rfm:'))
        .map((t) => ({ id: t.id }));

      const tags: any[] = [...keptTags, { name: newTag }];

      const patch: any = {
        id: u.contactId,
        _embedded: { tags },
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
