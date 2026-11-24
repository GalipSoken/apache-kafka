"""
AdminClient ile topic oluşturma / listeleme / açıklama.
Dosya: code/python/00_admin_topics.py
Çalıştırma: python 00_admin_topics.py
Beklenen çıktı: "Oluşturuldu: islemler" ... veya "Zaten var: islemler"
"""
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

from common import base_config, setup_logging

log = setup_logging("admin")

TOPICS = [
    NewTopic("islemler", num_partitions=6, replication_factor=1),
    NewTopic("islemler.json", num_partitions=6, replication_factor=1),
    NewTopic("islemler.DLT", num_partitions=1, replication_factor=1,
             config={"retention.ms": str(30 * 24 * 3600 * 1000)}),
    NewTopic("islemler.sonuc", num_partitions=6, replication_factor=1),
    NewTopic("musteri.profil", num_partitions=3, replication_factor=1,
             config={"cleanup.policy": "compact"}),
    NewTopic("kur.tcmb", num_partitions=1, replication_factor=1),
    NewTopic("csv.yukleme", num_partitions=3, replication_factor=1),
]


def main() -> None:
    admin = AdminClient(base_config())
    futures = admin.create_topics(TOPICS, request_timeout=15)
    for name, fut in futures.items():
        try:
            fut.result()
            log.info("Oluşturuldu: %s", name)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Zaten var: %s", name)
            else:
                raise

    md = admin.list_topics(timeout=10)
    for t in sorted(md.topics):
        if t.startswith("_"):
            continue
        parts = md.topics[t].partitions
        leaders = {p: parts[p].leader for p in parts}
        log.info("%-18s partition=%d leaders=%s", t, len(parts), leaders)


if __name__ == "__main__":
    main()
