"""
4) JSON Consumer: JSON'u Decimal hassasiyetiyle çözer, header'ları okur.
Dosya: code/python/04_json_consumer.py
Çalıştırma: python 04_json_consumer.py
Beklenen çıktı: "EFT 12345.67 TRY musteri=17 partition=3 schema=islem.v1"
"""
from confluent_kafka import Consumer

from common import ShutdownFlag, consumer_config, from_json_bytes, setup_logging

log = setup_logging("json-consumer")


def main() -> None:
    c = Consumer(consumer_config("rehber-json"))
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Hata: %s", msg.error())
                continue
            try:
                olay = from_json_bytes(msg.value())
            except ValueError as e:
                log.error("Bozuk JSON offset=%d: %s", msg.offset(), e)
                c.commit(message=msg, asynchronous=False)  # zehirli mesajı atla (DLT için 13'e bakın)
                continue
            headers = dict(msg.headers() or [])
            log.info("%s %s %s musteri=%s partition=%d schema=%s",
                     olay["islem_tipi"], olay["tutar"], olay["para_birimi"], olay["musteri_id"],
                     msg.partition(), headers.get("schema", b"?").decode())
            c.commit(message=msg, asynchronous=True)
    finally:
        c.close()


if __name__ == "__main__":
    main()
