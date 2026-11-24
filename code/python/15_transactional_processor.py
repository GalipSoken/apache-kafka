"""
15) Transactional Producer: consume -> transform -> produce akışında Kafka içi exactly-once.
Dosya: code/python/15_transactional_processor.py
Çalıştırma: python 15_transactional_processor.py --instance 0
Çıktıyı yalnız COMMIT edilmiş mesajlarla okumak:
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
     --topic islemler.sonuc --from-beginning --isolation-level read_committed

Çalışma mantığı:
  begin_transaction()
     produce(sonuç mesajları)
     send_offsets_to_transaction(girdi offset'leri)   <- offset commit de transaction'ın parçası
  commit_transaction()
Ya çıktılar + offset'ler birlikte görünür ya da hiçbiri (abort).

transactional.id her uygulama instance'ı için SABİT ve BENZERSİZ olmalı (ör. pod adı).
Aynı id ile yeni instance açılınca eskisi "fenced" edilir (zombie koruması).
Kapsam dışı: Kafka dışındaki sistemler (PostgreSQL, REST API) bu transaction'a dahil DEĞİLDİR.
"""
import argparse
from decimal import Decimal

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from common import (ShutdownFlag, consumer_config, from_json_bytes, producer_config,
                    setup_logging, to_json_bytes)

log = setup_logging("txn")
ESIK = Decimal("100000")


def zenginlestir(o: dict) -> dict:
    o["buyuk_islem"] = Decimal(str(o["tutar"])) >= ESIK
    o["risk_puani"] = 80 if o["buyuk_islem"] and o.get("kanal") == "INTERNET" else 10
    return o


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance", default="0")
    ap.add_argument("--batch", type=int, default=200)
    a = ap.parse_args()

    c = Consumer(consumer_config("rehber-txn", **{
        "isolation.level": "read_committed",   # upstream'de abort edilmiş mesajları görme
        "enable.auto.commit": False,
    }))
    p = Producer(producer_config(**{"transactional.id": f"rehber-txn-processor-{a.instance}"}))
    p.init_transactions(30)                       # PID al, yarım kalan eski transaction'ları kapat
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)

    try:
        while flag.running:
            msgs = c.consume(num_messages=a.batch, timeout=1.0)
            msgs = [m for m in msgs if m.error() is None]
            if not msgs:
                continue
            p.begin_transaction()
            try:
                for m in msgs:
                    o = from_json_bytes(m.value())
                    if not isinstance(o, dict) or "tutar" not in o:
                        continue
                    p.produce("islemler.sonuc", key=m.key(), value=to_json_bytes(zenginlestir(o)))
                p.send_offsets_to_transaction(c.position(c.assignment()),
                                              c.consumer_group_metadata(), 30)
                p.commit_transaction(30)
                log.info("transaction commit: %d mesaj", len(msgs))
            except KafkaException as e:
                err: KafkaError = e.args[0]
                if err.txn_requires_abort():
                    log.warning("Transaction abort ediliyor: %s", err)
                    p.abort_transaction(30)
                    # Consumer'ı son commit edilmiş offset'lere geri sar
                    committed = c.committed(c.assignment(), timeout=10)
                    for tp in committed:
                        if tp.offset >= 0:
                            c.seek(tp)
                elif err.retriable():
                    log.warning("Geçici hata, tekrar denenecek: %s", err)
                else:
                    log.critical("Fatal (ör. fenced): %s", err)
                    raise
    finally:
        c.close()


if __name__ == "__main__":
    main()
