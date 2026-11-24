#!/usr/bin/env bash
# Kaynak tabloda INSERT/UPDATE/DELETE yapar; hedefte sonucu gösterir.
# Dosya: code/scripts/cdc_demo_changes.sh
set -euo pipefail
SRC="docker compose -f code/docker/docker-compose.cdc.yml exec -T pg-source psql -U postgres -d bankdb -v ON_ERROR_STOP=1"
TGT="docker compose -f code/docker/docker-compose.cdc.yml exec -T pg-target psql -U postgres -d dwh"

$SRC <<'SQL'
INSERT INTO hesap_hareket (hareket_id, musteri_id, islem_tipi, tutar, islem_zamani, aciklama)
VALUES (1004, 1, 'FAST', 350.40, now(), 'CDC insert testi');
UPDATE hesap_hareket SET tutar = 99000.00, aciklama = 'Tutar düzeltildi' WHERE hareket_id = 1002;
DELETE FROM hesap_hareket WHERE hareket_id = 1001;
SQL

echo ">> 10 sn bekleniyor (Debezium -> Kafka -> JDBC Sink)"; sleep 10
$TGT -c "SELECT hareket_id, islem_tipi, tutar, aciklama FROM hesap_hareket_replica ORDER BY hareket_id;"
