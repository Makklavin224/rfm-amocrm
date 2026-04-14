#!/bin/bash
# Установка RFM-сегментации на Ubuntu 20.04+ VPS
# Запуск: curl -sL <url>/setup.sh | bash
# Или:    bash setup.sh

set -e

APP_DIR="/opt/rfm-amocrm"
NODE_VERSION="20"

echo "=== RFM amoCRM Setup ==="

# 1. Node.js
if ! command -v node &>/dev/null; then
  echo "Installing Node.js ${NODE_VERSION}..."
  curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi
echo "Node: $(node -v)"

# 2. Копируем проект
echo "Setting up app in ${APP_DIR}..."
sudo mkdir -p ${APP_DIR}
sudo cp -r . ${APP_DIR}/
sudo chown -R $(whoami):$(whoami) ${APP_DIR}
cd ${APP_DIR}

# 3. Зависимости
npm install --production
npm install -g tsx

# 4. .env файл
if [ ! -f .env ]; then
  echo ""
  echo "=== Настройка .env ==="
  read -p "AMO_TOKEN (длинный JWT): " AMO_TOKEN
  cat > .env <<EOF
AMO_BASE_URL=https://avenue1.amocrm.ru
AMO_TOKEN=${AMO_TOKEN}
AMO_PIPELINE_ID=379278
EOF
  echo ".env создан"
else
  echo ".env уже существует"
fi

# 5. Тестовый прогон (только чтение)
echo ""
echo "=== Тестовый прогон (dry run) ==="
source .env && AMO_BASE_URL=$AMO_BASE_URL AMO_TOKEN=$AMO_TOKEN AMO_PIPELINE_ID=$AMO_PIPELINE_ID npx tsx run.ts --dry-run 2>&1 | head -20

# 6. Cron
CRON_CMD="cd ${APP_DIR} && source .env && AMO_BASE_URL=\$AMO_BASE_URL AMO_TOKEN=\$AMO_TOKEN AMO_PIPELINE_ID=\$AMO_PIPELINE_ID npx tsx run.ts >> /var/log/rfm-amocrm.log 2>&1"
CRON_LINE="0 2 * * * ${CRON_CMD}"

if crontab -l 2>/dev/null | grep -q "rfm-amocrm"; then
  echo "Cron уже настроен"
else
  (crontab -l 2>/dev/null; echo "${CRON_LINE}") | crontab -
  echo "Cron добавлен: ежедневно в 02:00"
fi

echo ""
echo "=== Готово ==="
echo "Лог: /var/log/rfm-amocrm.log"
echo "Ручной запуск: cd ${APP_DIR} && source .env && npx tsx run.ts"
echo "Cron: crontab -l"
