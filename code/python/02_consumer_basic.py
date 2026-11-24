"""
2) Basit Consumer: "islemler" topic'ini okur, ekrana basar.
Dosya: code/python/02_consumer_basic.py
Çalıştırma: python 02_consumer_basic.py   (Ctrl+C ile durdurun)
Beklenen çıktı: "islemler[2]@0 key=None value=merhaba kafka #0" ...
Not: Bu örnek sadelik için enable.auto.commit=True kullanır. Production'da 10_manual_commit.py'ye bakın.
"""
from confluent_kafka import Consumer, KafkaError

from common import ShutdownFlag, consumer_config, setup_logging

log = setup_logging("consumer-basic")


def main() -> None:
    c = Consumer(consumer_config("rehber-basic", **{"enable.auto.commit": True}))
    c.subscribe(["islemler"])
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msg = c.poll(1.0)                 # en fazla 1 sn bekler
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer hatası: %s", msg.error())
                continue
            log.info("%s[%d]@%d key=%s value=%s", msg.topic(), msg.partition(),
                     msg.offset(), msg.key(), msg.value().decode())
    finally:
        c.close()   # grup'tan düzgün ayrılır, auto-commit açıksa son offset'leri yazar


if __name__ == "__main__":
    main()
