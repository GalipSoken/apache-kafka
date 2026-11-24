#!/usr/bin/env bash
# Rehberdeki Python örneklerinin kullandığı topic'leri oluşturur.
# Dosya: code/scripts/create_topics.sh
# Ön koşul: code/docker/docker-compose.yml ayakta olmalı (container adı: kafka)
set -euo pipefail
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092"

create () {  # create <topic> <partitions> [ek config...]
  local t=$1 p=$2; shift 2
  $KT --create --if-not-exists --topic "$t" --partitions "$p" --replication-factor 1 "$@"
}

create islemler          6
create islemler.json     6
create islemler.DLT      1 --config retention.ms=2592000000      # 30 gün
create islemler.sonuc    6
create musteri.profil    3 --config cleanup.policy=compact
create kur.tcmb          1
create csv.yukleme       3

$KT --list
