"""
6) Kafka'dan CSV dosyasına yazma (dosya rotasyonu + commit-after-flush).
Dosya: code/python/06_kafka_to_csv.py
Çalıştırma: python 06_kafka_to_csv.py --cikti ./cikti --satir-limit 500
Beklenen çıktı: ./cikti/islemler_20261124T201500Z_0001.csv dosyaları
Garanti: Offset ancak CSV dosyası diske yazılıp kapatıldıktan sonra commit edilir
(at-least-once). Çökme halinde son dosya tekrar yazılabilir -> dosya adı benzersizdir.
"""
import argparse
import csv
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer

from common import ShutdownFlag, consumer_config, from_json_bytes, setup_logging

log = setup_logging("kafka-to-csv")
ALANLAR = ["hareket_id", "musteri_id", "islem_tipi", "tutar", "para_birimi", "islem_zamani",
           "kafka_partition", "kafka_offset"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="islemler.json")
    ap.add_argument("--cikti", default="./cikti")
    ap.add_argument("--satir-limit", type=int, default=500)
    args = ap.parse_args()
    os.makedirs(args.cikti, exist_ok=True)

    c = Consumer(consumer_config("rehber-csv-export"))
    c.subscribe([args.topic])
    flag = ShutdownFlag().install(log)
    buf: list[dict] = []
    dosya_no = 0

    def flush_dosya():
        nonlocal buf, dosya_no
        if not buf:
            return
        dosya_no += 1
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        yol = os.path.join(args.cikti, f"islemler_{ts}_{dosya_no:04d}.csv")
        gecici = yol + ".tmp"
        with open(gecici, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ALANLAR, extrasaction="ignore")
            w.writeheader()
            w.writerows(buf)
            f.flush()
            os.fsync(f.fileno())
        os.replace(gecici, yol)          # atomik: yarım dosya görünmez
        c.commit(asynchronous=False)     # dosya kalıcı olduktan SONRA offset
        log.info("%s yazıldı (%d satır)", yol, len(buf))
        buf = []

    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None:
                if buf:
                    flush_dosya()        # boşta kalınca bekleyeni yaz
                continue
            if msg.error():
                log.error("%s", msg.error())
                continue
            kayit = from_json_bytes(msg.value())
            kayit["kafka_partition"] = msg.partition()
            kayit["kafka_offset"] = msg.offset()
            buf.append(kayit)
            if len(buf) >= args.satir_limit:
                flush_dosya()
        flush_dosya()
    finally:
        c.close()


if __name__ == "__main__":
    main()
