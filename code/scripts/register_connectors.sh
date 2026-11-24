#!/usr/bin/env bash
# Debezium source + JDBC sink connector'larını Kafka Connect REST API'ye kaydeder.
# Dosya: code/scripts/register_connectors.sh
# Çalıştırma (repo kökünden): bash code/scripts/register_connectors.sh
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")/../connect" && pwd)"

echo ">> Connect worker hazır mı bekleniyor: $CONNECT_URL"
for i in $(seq 1 60); do
  if curl -fs "$CONNECT_URL/" > /dev/null; then break; fi
  sleep 2
done

echo ">> Yüklü plugin'ler (PostgreSQL ve JDBC görünmeli):"
curl -fs "$CONNECT_URL/connector-plugins" | python3 -c 'import sys,json;[print("  ",p["class"],p.get("version")) for p in json.load(sys.stdin) if "postgres" in p["class"].lower() or "jdbc" in p["class"].lower()]'

register () {
  local file="$1"
  local name
  name=$(python3 -c "import json,sys;print(json.load(open('$file'))['name'])")
  # PUT /connectors/{name}/config idempotenttir: yoksa oluşturur, varsa günceller.
  python3 -c "import json;print(json.dumps(json.load(open('$file'))['config']))" \
    | curl -fs -X PUT -H "Content-Type: application/json" --data @- \
      "$CONNECT_URL/connectors/$name/config" > /dev/null
  echo ">> Kaydedildi: $name"
}

register "$DIR/pg-source-debezium.json"
sleep 5
register "$DIR/pg-sink-debezium-jdbc.json"

sleep 5
for c in pg-source-bank pg-sink-dwh; do
  echo ">> Durum: $c"
  curl -fs "$CONNECT_URL/connectors/$c/status" | python3 -m json.tool
done
