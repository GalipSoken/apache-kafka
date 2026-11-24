"""
13) Dead Letter Topic (DLT): retry'lar tükenince veya kalıcı hatada mesajı DLT'ye yolla,
offset'i ilerlet, akışı DURDURMA.
Dosya: code/python/13_dead_letter_topic.py
Çalıştırma: python 13_dead_letter_topic.py
Test için bozuk mesaj gönderin:
   echo 'bozuk{json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:19092 --topic islemler.json
Beklenen çıktı: "DLT'ye gönderildi islemler.json[2]@57 sebep=JSONDecodeError"
DLT'yi incelemek:
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
        --topic islemler.DLT --from-beginning --property print.headers=true
"""
from decimal import Decimal

from confluent_kafka import Consumer, Producer

from common import (NonRetriableError, RetriableError, ShutdownFlag, consumer_config,
                    dlt_headers, from_json_bytes, producer_config, retry_call,
                    safe_commit, setup_logging)

log = setup_logging("dlt")
DLT_TOPIC = "islemler.DLT"
MAX_DENEME = 3


def isle(raw: bytes) -> None:
    try:
        olay = from_json_bytes(raw)
    except ValueError as e:                       # JSONDecodeError ValueError alt sınıfıdır
        raise NonRetriableError(f"JSONDecodeError: {e}") from e
    if not isinstance(olay, dict) or "tutar" not in olay:
        raise NonRetriableError("şema ihlali: tutar alanı yok")
    if Decimal(olay["tutar"]) <= 0:
        raise NonRetriableError("iş kuralı: tutar pozitif olmalı")
    # ... burada DB / servis çağrısı (RetriableError fırlatabilir)


def main() -> None:
    c = Consumer(consumer_config("rehber-dlt"))
    dlt = Producer(producer_config(**{"linger.ms": 0}))
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                retry_call(lambda: isle(msg.value()), attempts=MAX_DENEME,
                           retry_on=(RetriableError,), log=log)
            except (NonRetriableError, RetriableError) as exc:
                dlt.produce(DLT_TOPIC, key=msg.key(), value=msg.value(),
                            headers=dlt_headers(msg, exc, MAX_DENEME))
                # DLT'ye yazıldığından emin olmadan offset'i ilerletmiyoruz
                if dlt.flush(10) > 0:
                    raise RuntimeError("DLT'ye yazılamadı; offset commit edilmeyecek")
                log.warning("DLT'ye gönderildi %s[%d]@%d sebep=%s",
                            msg.topic(), msg.partition(), msg.offset(), exc)
            safe_commit(c, log, message=msg)
    finally:
        dlt.flush(10)
        c.close()


if __name__ == "__main__":
    main()
