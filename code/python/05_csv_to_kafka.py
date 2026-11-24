"""
5) CSV dosyasından Kafka'ya veri gönderme (satır = olay).
Dosya: code/python/05_csv_to_kafka.py
Çalıştırma: python 05_csv_to_kafka.py --dosya ../data/islemler_ornek.csv
Beklenen çıktı: "20 satır gönderildi, 0 satır reddedildi"
Not: Pandas'sız, akış (streaming) halinde okur; çok büyük CSV'lerde bellek sabit kalır.
"""
import argparse
import csv
from decimal import Decimal, InvalidOperation

from confluent_kafka import Producer

from common import make_delivery_callback, producer_config, setup_logging, to_json_bytes

log = setup_logging("csv-to-kafka")
ZORUNLU = ["hareket_id", "musteri_id", "islem_tipi", "tutar", "para_birimi", "islem_zamani"]


def satir_dogrula(row: dict) -> dict:
    eksik = [k for k in ZORUNLU if not row.get(k)]
    if eksik:
        raise ValueError(f"eksik alan: {eksik}")
    try:
        row["tutar"] = Decimal(row["tutar"].replace(",", "."))
    except InvalidOperation as e:
        raise ValueError(f"tutar sayı değil: {row['tutar']}") from e
    row["hareket_id"] = int(row["hareket_id"])
    row["musteri_id"] = int(row["musteri_id"])
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dosya", required=True)
    ap.add_argument("--topic", default="csv.yukleme")
    args = ap.parse_args()

    p = Producer(producer_config())
    cb = make_delivery_callback(log)
    ok = red = 0
    with open(args.dosya, newline="", encoding="utf-8") as f:
        for satir_no, row in enumerate(csv.DictReader(f), start=2):
            try:
                kayit = satir_dogrula(row)
            except ValueError as e:
                red += 1
                log.warning("Satır %d reddedildi: %s", satir_no, e)
                continue
            p.produce(args.topic, key=str(kayit["hareket_id"]).encode(),
                      value=to_json_bytes(kayit), on_delivery=cb,
                      headers=[("kaynak.dosya", args.dosya.encode()), ("kaynak.satir", str(satir_no).encode())])
            p.poll(0)
            ok += 1
    p.flush(30)
    log.info("%d satır gönderildi, %d satır reddedildi", ok, red)


if __name__ == "__main__":
    main()
