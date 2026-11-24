"""
3) JSON Producer: Decimal/datetime güvenli JSON ile işlem olayları gönderir.
Dosya: code/python/03_json_producer.py
Çalıştırma: python 03_json_producer.py --adet 1000
Beklenen çıktı: "1000 işlem gönderildi (x.x msg/sn)"
"""
import argparse
import time

from confluent_kafka import Producer

from common import make_delivery_callback, producer_config, setup_logging, to_json_bytes
from txn_data import sentetik_islem

log = setup_logging("json-producer")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--adet", type=int, default=1000)
    ap.add_argument("--topic", default="islemler.json")
    args = ap.parse_args()

    p = Producer(producer_config())
    cb = make_delivery_callback(log)
    t0 = time.perf_counter()
    for i in range(1, args.adet + 1):
        olay = sentetik_islem(i)
        while True:
            try:
                p.produce(
                    args.topic,
                    key=str(olay["musteri_id"]).encode(),          # aynı müşteri -> aynı partition -> sıralı
                    value=to_json_bytes(olay),
                    timestamp=olay["islem_ts_ms"],                  # Kafka record timestamp = event time
                    headers=[("schema", b"islem.v1"), ("kaynak", b"03_json_producer")],
                    on_delivery=cb,
                )
                break
            except BufferError:
                # Yerel kuyruk doldu: callback'leri işletip yer aç
                p.poll(0.5)
        p.poll(0)
    p.flush(30)
    sure = time.perf_counter() - t0
    log.info("%d işlem gönderildi (%.1f msg/sn)", args.adet, args.adet / sure)


if __name__ == "__main__":
    main()
