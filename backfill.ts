// Одноразовый скрипт: подгружает исторические продажи из сделок
// "Физ. отдел" как транзакции в существующих покупателей.
// Использует external_id = "deal_{id}" для идемпотентности.

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;
const PIPELINE_ID = process.env.AMO_PIPELINE_ID || '379278';

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

const CONCURRENT = 5;

interface Deal {
  id: number;
  price: number;
  closed_at: number;
  contact_id: number;
}

// ─── Загружаем выигранные сделки из "Физ. отдел" ───────

async function fetchWonDeals(): Promise<Deal[]> {
  const deals: Deal[] = [];
  let page = 1;
  let keepGoing = true;

  while (keepGoing) {
    const pages = Array.from({ length: CONCURRENT }, (_, i) => page + i);

    const results = await Promise.all(
      pages.map(async (p) => {
        const url = `${BASE_URL}/api/v4/leads?limit=250&page=${p}&with=contacts&filter[statuses][0][pipeline_id]=${PIPELINE_ID}&filter[statuses][0][status_id]=142`;
        const res = await fetch(url, { headers });
        if (res.status === 204 || !res.ok) return { leads: [], hasNext: false };
        const data = await res.json();
        return {
          leads: data?._embedded?.leads || [],
          hasNext: !!data._links?.next,
        };
      })
    );

    for (const result of results) {
      for (const lead of result.leads) {
        const mainContact = lead._embedded?.contacts?.find(
          (c: any) => c.is_main
        );
        if (!mainContact || !lead.closed_at || !lead.price) continue;
        deals.push({
          id: lead.id,
          price: lead.price,
          closed_at: lead.closed_at,
          contact_id: mainContact.id,
        });
      }
      if (!result.hasNext || result.leads.length === 0) keepGoing = false;
    }

    page += CONCURRENT;
    if (keepGoing) await sleep(200);
  }

  return deals;
}

// ─── Map: contact_id → customer_id ───────────────────────

async function fetchContactToCustomer(): Promise<Map<number, number>> {
  const map = new Map<number, number>();
  let page = 1;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`;
    const res = await fetch(url, { headers });
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();

    for (const cust of data?._embedded?.customers || []) {
      for (const contact of cust._embedded?.contacts || []) {
        map.set(contact.id, cust.id);
      }
    }

    if (!data._links?.next) break;
    page++;
    await sleep(150);
  }

  return map;
}

// ─── Получить уже импортированные deal_id из комментариев ─

async function fetchImportedDealIds(
  customerId: number
): Promise<Set<number>> {
  const ids = new Set<number>();
  let page = 1;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers/${customerId}/transactions?limit=250&page=${page}`;
    const res = await fetch(url, { headers });
    if (res.status === 204 || !res.ok) break;
    const data = await res.json();

    for (const tx of data?._embedded?.transactions || []) {
      if (tx.comment) {
        const m = tx.comment.match(/\[DEAL:(\d+)\]/);
        if (m) ids.add(parseInt(m[1]));
      }
    }

    if (!data._links?.next) break;
    page++;
  }

  return ids;
}

// ─── Добавить транзакции покупателю ──────────────────────

async function addTransactions(
  customerId: number,
  txs: Array<{ price: number; completed_at: number; dealId: number }>
): Promise<number> {
  if (txs.length === 0) return 0;

  const res = await fetch(
    `${BASE_URL}/api/v4/customers/${customerId}/transactions`,
    {
      method: 'POST',
      headers,
      body: JSON.stringify(
        txs.map((t) => ({
          price: t.price,
          completed_at: t.completed_at,
          comment: `[DEAL:${t.dealId}] Импорт из сделки`,
        }))
      ),
    }
  );

  if (!res.ok) {
    const text = await res.text();
    console.error(`  Add tx error customer=${customerId}: ${res.status} ${text.slice(0, 200)}`);
    return 0;
  }

  return txs.length;
}

// ─── Main ───────────────────────────────────────────────

async function main() {
  const start = Date.now();
  const ts = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  console.log('=== Backfill: сделки → транзакции покупателей ===\n');

  // 1. Выигранные сделки
  console.log(`[${ts()}] Fetching won deals...`);
  const deals = await fetchWonDeals();
  console.log(`[${ts()}] Loaded ${deals.length} won deals`);

  // 2. Карта contact → customer
  console.log(`[${ts()}] Fetching customers with contacts...`);
  const contactToCustomer = await fetchContactToCustomer();
  console.log(`[${ts()}] Found ${contactToCustomer.size} contact→customer links`);

  // 3. Группируем сделки по customer_id
  const byCustomer = new Map<number, Deal[]>();
  let orphans = 0;

  for (const deal of deals) {
    const customerId = contactToCustomer.get(deal.contact_id);
    if (!customerId) {
      orphans++;
      continue;
    }
    if (!byCustomer.has(customerId)) byCustomer.set(customerId, []);
    byCustomer.get(customerId)!.push(deal);
  }

  console.log(`[${ts()}] Grouped: ${byCustomer.size} customers, ${orphans} orphan deals (no customer)`);

  // 4. Для каждого покупателя добавляем транзакции
  let totalAdded = 0;
  let totalSkipped = 0;
  let processed = 0;

  const entries = [...byCustomer.entries()];
  const chunks: Array<[number, Deal[]][]> = [];
  for (let i = 0; i < entries.length; i += CONCURRENT) {
    chunks.push(entries.slice(i, i + CONCURRENT));
  }

  for (const chunk of chunks) {
    await Promise.all(
      chunk.map(async ([customerId, customerDeals]) => {
        // Идемпотентность: смотрим уже импортированные по [DEAL:X] в comment
        const importedIds = await fetchImportedDealIds(customerId);

        const toAdd = customerDeals
          .filter((d) => !importedIds.has(d.id))
          .map((d) => ({
            price: d.price,
            completed_at: d.closed_at,
            dealId: d.id,
          }));

        totalSkipped += customerDeals.length - toAdd.length;

        if (toAdd.length > 0) {
          const added = await addTransactions(customerId, toAdd);
          totalAdded += added;
        }
      })
    );

    processed += chunk.length;
    if (processed % 25 === 0) {
      console.log(`[${ts()}] ${processed}/${byCustomer.size} customers processed (added: ${totalAdded})`);
    }

    await sleep(300);
  }

  console.log(`\n[${ts()}] DONE`);
  console.log(`  Customers processed: ${byCustomer.size}`);
  console.log(`  Transactions added:  ${totalAdded}`);
  console.log(`  Transactions skipped (already imported): ${totalSkipped}`);
  console.log(`  Orphan deals (no customer): ${orphans}`);
}

main().catch(console.error);
