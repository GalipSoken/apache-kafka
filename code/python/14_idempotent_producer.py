"""
14) Idempotent Producer: retry kaynaklı DUPLICATE ve SIRA BOZULMASINI broker tarafında engeller.
Dosya: code/python/14_idempotent_producer.py
Çalıştırma: python 14_idempotent_producer.py

Nasıl çalışır: Broker her producer'a bir Producer ID (PID) verir; producer her partition için
artan bir sequence number gönderir. Ağ hatası nedeniyle aynı batch iki kez gelirse broker
(PID, partition, sequence) üçlüsünden tekrarı anlar ve ikinciyi yazmaz.

Kapsam: Tek producer oturumu + tek partition seviyesinde exactly-once YAZIM sağlar.
Uygulama aynı iş olayını iki kez produce() ederse (ör. çökme sonrası yeniden başlatma)
bunu ENGELLEMEZ -> o durum için transaction (15) veya tüketici tarafında idempotency gerekir.
"""
from confluent_kafka import KafkaError, KafkaException, Producer

from common import make_delivery_callback, producer_config, setup_logging, to_json_bytes
from txn_data import sentetik_islem

log = setup_logging("idempotent")


def main() -> None:
    cfg = producer_config(**{
        "enable.idempotence": True,     # şunları zorunlu kılar/ayarlar:
        "acks": "all",                  #   acks=all
        "max.in.flight.requests.per.connection": 5,   # <= 5 olmalı; sıralama yine korunur
        "retries": 2147483647,          #   sınırsız retry (delivery.timeout.ms ile sınırlı)
    })
    p = Producer(cfg)
    cb = make_delivery_callback(log)
    try:
        for i in range(1, 501):
            o = sentetik_islem(i)
            p.produce("islemler.json", key=str(o["musteri_id"]).encode(), value=to_json_bytes(o), on_delivery=cb)
            p.poll(0)
        p.flush(30)
        log.info("500 mesaj idempotent producer ile gönderildi")
    except KafkaException as e:
        err: KafkaError = e.args[0]
        if err.fatal():
            # Idempotent producer'da fatal hata (ör. OUT_OF_ORDER_SEQUENCE) -> producer yeniden yaratılmalı
            log.critical("FATAL producer hatası, süreç yeniden başlatılmalı: %s", err)
        raise


if __name__ == "__main__":
    main()
