// Одноразовый скрипт: привязывает контакты к покупателям,
// у которых имя = "Покупатель (контакт #XXXXX)" но contacts=[]

const BASE_URL = process.env.AMO_BASE_URL!;
const TOKEN = process.env.AMO_TOKEN!;

const headers = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  console.log('=== Fix: linking contacts to customers ===\n');

  let page = 1;
  let fixed = 0;
  let skipped = 0;

  while (true) {
    const url = `${BASE_URL}/api/v4/customers?limit=250&page=${page}&with=contacts`;
    const res = await fetch(url, { headers });

    if (res.status === 204) break;
    if (!res.ok) break;

    const data = await res.json();
    const customers = data?._embedded?.customers || [];

    for (const cust of customers) {
      const contacts = cust._embedded?.contacts || [];

      // Уже привязан — пропускаем
      if (contacts.length > 0) {
        skipped++;
        continue;
      }

      // Парсим contact_id из имени "Покупатель (контакт #XXXXX)"
      const match = cust.name.match(/контакт #(\d+)/);
      if (!match) {
        skipped++;
        continue;
      }

      const contactId = parseInt(match[1]);

      const linkRes = await fetch(
        `${BASE_URL}/api/v4/customers/${cust.id}/link`,
        {
          method: 'POST',
          headers,
          body: JSON.stringify([
            { to_entity_id: contactId, to_entity_type: 'contacts' },
          ]),
        }
      );

      if (linkRes.ok) {
        fixed++;
        if (fixed % 50 === 0) console.log(`  Linked ${fixed}...`);
      } else {
        const text = await linkRes.text();
        console.error(`  Failed customer=${cust.id} contact=${contactId}: ${text}`);
      }

      await sleep(150);
    }

    if (!data._links?.next) break;
    page++;
  }

  console.log(`\nDone. Fixed: ${fixed}, skipped: ${skipped}`);
}

main().catch(console.error);
