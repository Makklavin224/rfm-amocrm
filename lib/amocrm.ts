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

// ─── Получить все выигранные сделки (параллельно) ───────

const CONCURRENT = 5;

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
    const pages = Array.from({ length: CONCURRENT }, (_, i) => page + i);
    const results = await Promise.all(pages.map((p) => fetchPage(p)));

    for (const result of results) {
      deals.push(...parseLeads(result.leads));
      if (!result.hasNext || result.leads.length === 0) {
        keepGoing = false;
      }
    }

    page += CONCURRENT;
    if (keepGoing) await sleep(200);
  }

  return deals;
}

// ─── Покупатели: загрузить всех с привязанными контактами ─

export async function fetchAllCustomers(): Promise<Map<number, number>> {
  // Возвращает Map<contact_id, customer_id>
  const contactToCustomer = new Map<number, number>();
  let page = 1;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`;
    const res = await fetch(url, { headers });

    if (res.status === 204) break;
    if (!res.ok) break;

    const data = await res.json();
    const customers = data?._embedded?.customers || [];

    for (const cust of customers) {
      const contacts = cust._embedded?.contacts || [];
      for (const contact of contacts) {
        contactToCustomer.set(contact.id, cust.id);
      }
    }

    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }

  return contactToCustomer;
}

// ─── Покупатели: создать новых для контактов без покупателя ─

export async function createCustomers(
  contactIds: number[],
  segmentMap: Map<number, string>
): Promise<Map<number, number>> {
  const created = new Map<number, number>(); // contact_id → new customer_id
  const chunks = chunkArray(contactIds, 50);

  for (const chunk of chunks) {
    const body = chunk.map((contactId) => {
      const segment = segmentMap.get(contactId);
      const segmentId = segment ? SEGMENT_IDS[segment] : undefined;

      const customer: any = {
        name: `${contactId}`,
      };

      if (segmentId) {
        customer._embedded = { segments: [{ id: segmentId }] };
      }

      return customer;
    });

    const res = await fetch(`${BASE_URL}/api/v4/customers`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    if (res.ok) {
      const data = await res.json();
      const customers = data?._embedded?.customers || [];
      for (let i = 0; i < customers.length && i < chunk.length; i++) {
        created.set(chunk[i], customers[i].id);
      }
    } else {
      const text = await res.text();
      console.error(`Create customers error: ${res.status} ${text}`);
    }

    await sleep(300);

    // Привязываем контакты через /link endpoint
    for (let i = 0; i < chunk.length; i++) {
      const customerId = created.get(chunk[i]);
      if (!customerId) continue;

      await fetch(`${BASE_URL}/api/v4/customers/${customerId}/link`, {
        method: 'POST',
        headers,
        body: JSON.stringify([
          { to_entity_id: chunk[i], to_entity_type: 'contacts' },
        ]),
      });
      await sleep(150);
    }
  }

  return created;
}

// ─── Покупатели: привязать контакты к уже созданным ─────

export async function linkContactsToCustomers(
  pairs: Array<{ customerId: number; contactId: number }>
): Promise<number> {
  let linked = 0;

  for (const { customerId, contactId } of pairs) {
    const res = await fetch(`${BASE_URL}/api/v4/customers/${customerId}/link`, {
      method: 'POST',
      headers,
      body: JSON.stringify([
        { to_entity_id: contactId, to_entity_type: 'contacts' },
      ]),
    });

    if (res.ok) {
      linked++;
    } else {
      const text = await res.text();
      console.error(`Link error customer=${customerId} contact=${contactId}: ${res.status} ${text}`);
    }

    await sleep(150);
  }

  return linked;
}

// ─── Покупатели: обновить сегменты у существующих ────────

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
