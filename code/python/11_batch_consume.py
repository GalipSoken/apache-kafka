"""
11) Batch tüketimi: consume(num_messages=N) ile toplu okuma + PostgreSQL'e toplu idempotent upsert.
Dosya: code/python/11_batch_consume.py
Ön koşul: hedef DB'de public.islem_ham tablosu (code/sql/target_init.sql)
Çalıştırma:
   export PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"
   python 11_batch_consume.py --batch 500
Beklenen çıktı: "500 kayıt upsert edildi, offset commit edildi"
Garanti: DB transaction'ı commit edildikten SONRA Kafka offset'i commit edilir.
Çökme arada olursa batch tekrar gelir; ON CONFLICT sayesinde sonuç değişmez (idempotent).
"""
import argparse
import os
from datetime import datetime
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer

from common import ShutdownFlag, consumer_config, from_json_bytes, safe_commit, setup_logging

log = setup_logging("batch-consume")

UPSERT_SQL = """
INSERT INTO public.islem_ham
  (hareket_id, musteri_id, islem_tipi, tutar, para_birimi, islem_zamani,
   kafka_topic, kafka_partition, kafka_offset)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (hareket_id) DO UPDATE SET
   musteri_id = EXCLUDED.musteri_id,
   islem_tipi = EXCLUDED.islem_tipi,
   tutar = EXCLUDED.tutar,
   para_birimi = EXCLUDED.para_birimi,
   islem_zamani = EXCLUDED.islem_zamani,
   kafka_topic = EXCLUDED.kafka_topic,
   kafka_partition = EXCLUDED.kafka_partition,
   kafka_offset = EXCLUDED.kafka_offset,
   yuklendi = now()
WHERE public.islem_ham.kafka_offset <= EXCLUDED.kafka_offset   -- eski mesaj yeniyi ezmesin
"""


def satirlar(mesajlar):
    for m in mesajlar:
        o = from_json_bytes(m.value())
        if o is None or "hareket_id" not in o:
            continue
        # JSON'da Decimal string, zaman ISO-8601 string olarak taşınır -> açıkça dönüştür
        yield (int(o["hareket_id"]), int(o["musteri_id"]), o["islem_tipi"], Decimal(str(o["tutar"])),
               o["para_birimi"], datetime.fromisoformat(o["islem_zamani"]),
               m.topic(), m.partition(), m.offset())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--timeout", type=float, default=2.0)
    a = ap.parse_args()
    dsn = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5434/dwh")

    c = Consumer(consumer_config("rehber-batch-pg", **{"max.poll.interval.ms": 300000}))
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    with psycopg.connect(dsn) as conn:
        try:
            while flag.running:
                msgs = c.consume(num_messages=a.batch, timeout=a.timeout)
                if not msgs:
                    continue
                gecerli = [m for m in msgs if m.error() is None]
                rows = list(satirlar(gecerli))
                if rows:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.executemany(UPSERT_SQL, rows)
                safe_commit(c, log)
                log.info("%d kayıt upsert edildi, offset commit edildi", len(rows))
        finally:
            c.close()


if __name__ == "__main__":
    main()
