#!/bin/bash

echo "=== Verificação de Configuração de Timezone ==="
echo ""

echo "📅 Data e hora atual do container:"
date

echo ""
echo "🌍 Timezone configurado:"
cat /etc/timezone

echo ""
echo "🇧🇷 Locale configurado:"
locale | grep -E "(LANG|LC_TIME|LC_ALL)"

echo ""
echo "⏰ Data no formato brasileiro:"
date '+%d/%m/%Y %H:%M:%S'

echo ""
echo "🐍 Testando Python datetime:"
python3 -c "
import datetime
import os
print('TZ do sistema:', os.environ.get('TZ', 'Não definido'))
print('Data/hora atual:', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
print('Data ISO atual:', datetime.datetime.now().strftime('%Y-%m-%d'))
"

echo ""
echo "✅ Verificação concluída!"
