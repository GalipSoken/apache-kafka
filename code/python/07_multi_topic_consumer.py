"""
7) Birden fazla topic dinleme: liste ve regex abonelik.
Dosya: code/python/07_multi_topic_consumer.py
Çalıştırma: python 07_multi_topic_consumer.py
Regex abonelikte topic adı '^' ile başlamalıdır. Yeni oluşturulan ve desene uyan
topic'ler metadata yenilendiğinde (topic.metadata.refresh.interval.ms) otomatik eklenir.
"""
from confluent_kafka import Consumer

from common import ShutdownFlag, consumer_config, setup_logging

log = setup_logging("multi-topic")

HANDLERS = {
    "islemler.json": lambda m: log.info("İŞLEM  p%d o%d", m.partition(), m.offset()),
    "csv.yukleme":   lambda m: log.info("CSV    p%d o%d", m.partition(), m.offset()),
    "kur.tcmb":      lambda m: log.info("KUR    %s", m.value()[:80]),
}


def on_assign(consumer, partitions):
    log.info("Atanan partition'lar: %s", [(p.topic, p.partition) for p in partitions])


def main() -> None:
    c = Consumer(consumer_config("rehber-multi"))
    # Seçenek A: açık liste
    c.subscribe(["islemler.json", "csv.yukleme", "kur.tcmb"], on_assign=on_assign)
    # Seçenek B: regex (A yerine)
    # c.subscribe(["^islemler\\..*"], on_assign=on_assign)
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            HANDLERS.get(msg.topic(), lambda m: log.info("Diğer %s", m.topic()))(msg)
            c.commit(message=msg, asynchronous=True)
    finally:
        c.close()


if __name__ == "__main__":
    main()
