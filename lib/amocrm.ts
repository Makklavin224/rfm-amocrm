const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const PIPELINE_ID = process.env.AMO_PIPELINE_ID || '379278'; // Физ. отдел

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── Типы ───────────────────────────────────────────────

export interface AmoDeal {
  id: number;
  price: number;
  closed_at: number | null;
  status_id: number;
  contact_id: number | null;
  contact_name?: string;
}

export interface AmoContact {
  id: number;
  name: string;
  custom_fields_values: Array<{
    field_id: number;
    field_name: string;
    values: Array<{ value: string; enum_id?: number }>;
  }> | null;
  _embedded?: {
    tags?: Array<{ id: number; name: string }>;
  };
}

// ─── Получить все выигранные сделки (параллельно) ───────

const CONCURRENT = 5; // amoCRM rate limit ~7 req/s

async function fetchPage(page: number): Promise<{ leads: any[]; hasNext: boolean }> {
  const url = `${BASE_URL}/api/v4/leads?limit=250&page=${page}&with=contacts&filter[statuses][0][pipeline_id]=${PIPELINE_ID}&filter[statuses][0][status_id]=142`;
  const res = await fetch(url, { headers });

  if (res.status === 204) return { leads: [], hasNext: false };
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`amoCRM leads error ${res.status}: ${text}`);
  }

  const data = await res.json();
  return {
    leads: data?._embedded?.leads || [],
    hasNext: !!data._links?.next,
  };
}

function parseLeads(items: any[]): AmoDeal[] {
  return items.map((lead) => {
    const mainContact = lead._embedded?.contacts?.find((c: any) => c.is_main);
    return {
      id: lead.id,
      price: lead.price || 0,
      closed_at: lead.closed_at,
      status_id: lead.status_id,
      contact_id: mainContact?.id || null,
    };
  });
}

export async function fetchAllWonDeals(): Promise<AmoDeal[]> {
  const deals: AmoDeal[] = [];
  let page = 1;
  let keepGoing = true;

  while (keepGoing) {
    // Запускаем CONCURRENT страниц параллельно
    const pages = Array.from({ length: CONCURRENT }, (_, i) => page + i);
    const results = await Promise.all(pages.map((p) => fetchPage(p)));

    for (const result of results) {
      deals.push(...parseLeads(result.leads));
      if (!result.hasNext || result.leads.length === 0) {
        keepGoing = false;
      }
    }

    page += CONCURRENT;
    if (keepGoing) await sleep(200); // rate limit safety
  }

  return deals;
}

// ─── Получить имена контактов ───────────────────────────

export async function fetchContactNames(
  contactIds: number[]
): Promise<Map<number, string>> {
  const names = new Map<number, string>();
  const chunks = chunkArray(contactIds, 50);

  for (const chunk of chunks) {
    const ids = chunk.map((id) => `filter[id][]=${id}`).join('&');
    const url = `${BASE_URL}/api/v4/contacts?${ids}&limit=250`;
    const res = await fetch(url, { headers });

    if (res.status === 204) continue;
    if (!res.ok) continue;

    const data = await res.json();
    for (const contact of data?._embedded?.contacts || []) {
      names.set(contact.id, contact.name);
    }
    await sleep(150);
  }

  return names;
}

// ─── Найти или создать кастомное поле "RFM-сегмент" ─────

const RFM_SEGMENTS = [
  'VIP',
  'Киты',
  'Лояльные',
  'Перспективные',
  'Новичок',
  'В зоне риска',
  'VIP/КИТ в оттоке',
  'Потерянный',
  'Архив',
];

export async function ensureRfmField(): Promise<number> {
  // Проверяем, существует ли уже поле
  const listRes = await fetch(`${BASE_URL}/api/v4/contacts/custom_fields`, {
    headers,
  });

  if (listRes.ok) {
    const data = await listRes.json();
    const existing = data?._embedded?.custom_fields?.find(
      (f: any) => f.name === 'RFM-сегмент'
    );
    if (existing) return existing.id;
  }

  // Создаём поле типа select
  const createRes = await fetch(`${BASE_URL}/api/v4/contacts/custom_fields`, {
    method: 'POST',
    headers,
    body: JSON.stringify([
      {
        name: 'RFM-сегмент',
        type: 'select',
        enums: RFM_SEGMENTS.map((s) => ({ value: s })),
      },
    ]),
  });

  if (!createRes.ok) {
    const text = await createRes.text();
    throw new Error(`Failed to create custom field: ${createRes.status} ${text}`);
  }

  const result = await createRes.json();
  return result._embedded.custom_fields[0].id;
}

// ─── Получить enum_id для значения поля ─────────────────

export async function getFieldEnums(
  fieldId: number
): Promise<Map<string, number>> {
  const res = await fetch(
    `${BASE_URL}/api/v4/contacts/custom_fields/${fieldId}`,
    { headers }
  );

  if (!res.ok) return new Map();

  const data = await res.json();
  const enums = new Map<string, number>();
  for (const e of data.enums || []) {
    enums.set(e.value, e.id);
  }
  return enums;
}

// ─── Обновить контакты: записать сегмент + теги ─────────

export interface ContactUpdate {
  contactId: number;
  segment: string;
  fieldId: number;
  enumId: number;
}

export async function batchUpdateContacts(
  updates: ContactUpdate[]
): Promise<number> {
  let updated = 0;
  const chunks = chunkArray(updates, 50); // amoCRM batch limit

  for (const chunk of chunks) {
    const body = chunk.map((u) => ({
      id: u.contactId,
      custom_fields_values: [
        {
          field_id: u.fieldId,
          values: [{ enum_id: u.enumId }],
        },
      ],
      _embedded: {
        tags: [{ name: `rfm:${u.segment.toLowerCase().replace(/[\s\/]/g, '-')}` }],
      },
    }));

    const res = await fetch(`${BASE_URL}/api/v4/contacts`, {
      method: 'PATCH',
      headers,
      body: JSON.stringify(body),
    });

    if (res.ok) {
      updated += chunk.length;
    } else {
      const text = await res.text();
      console.error(`Batch update error: ${res.status} ${text}`);
    }

    await sleep(300); // rate limit safety
  }

  return updated;
}

// ─── Удалить старые rfm: теги у контактов ───────────────

export async function removeOldRfmTags(contactIds: number[]): Promise<void> {
  const chunks = chunkArray(contactIds, 50);

  for (const chunk of chunks) {
    // Получаем контакты с тегами
    const ids = chunk.map((id) => `filter[id][]=${id}`).join('&');
    const res = await fetch(
      `${BASE_URL}/api/v4/contacts?${ids}&limit=250`,
      { headers }
    );

    if (!res.ok || res.status === 204) continue;

    const data = await res.json();
    const updates: any[] = [];

    for (const contact of data._embedded?.contacts || []) {
      const rfmTags = contact._embedded?.tags?.filter((t: any) =>
        t.name.startsWith('rfm:')
      );
      if (rfmTags?.length) {
        updates.push({
          id: contact.id,
          _embedded: {
            tags: rfmTags.map((t: any) => ({
              id: t.id,
              name: t.name,
              _delete: true,
            })),
          },
        });
      }
    }

    if (updates.length > 0) {
      await fetch(`${BASE_URL}/api/v4/contacts`, {
        method: 'PATCH',
        headers,
        body: JSON.stringify(updates),
      });
    }

    await sleep(150);
  }
}

// ─── Утилиты ────────────────────────────────────────────

function chunkArray<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}
