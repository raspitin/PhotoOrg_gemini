#!/usr/bin/env bash
set -euo pipefail

if [ ! -d venv ]; then
echo "ℹ️ Ambiente virtuale non trovato, esegui prima bootstrap.sh"
exit 1
fi

echo "🐍 Attivo ambiente virtuale…"
source venv/bin/activate

echo "📦 Installazione/aggiornamento dipendenze Python da requirements.txt…"
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Setup completato. Puoi ora eseguire 'python3 PhotoOrg.py'"
echo ""
echo "################################"
echo ""
echo "Ora attiva l'ambiente eseguendo:"
echo "source venv/bin/activate"
echo ""
echo "################################"
echo ""
echo "Poi puoi eseguire:"
echo "python3 PhotoOrg.py"
