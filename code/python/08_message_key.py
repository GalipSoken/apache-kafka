"""
8) Message Key kullanma: aynı key -> aynı partition -> o key için sıralama garantisi.
Dosya: code/python/08_message_key.py
Çalıştırma: python 08_message_key.py
Beklenen çıktı: her musteri_id için hep AYNI partition numarası:
   musteri=7  -> partition 4 (5 kez)
"""
from collections import defaultdict

from confluent_kafka import Producer

from common import producer_config, setup_logging, to_json_bytes

log = setup_logging("message-key")


def main() -> None:
    p = Producer(producer_config())
    goruldu: dict[int, set[int]] = defaultdict(set)

    def cb(err, msg):
        if err is None:
            goruldu[int(msg.key())].add(msg.partition())

    for tur in range(5):
        for musteri in (3, 7, 11, 42):
            p.produce("islemler.json", key=str(musteri).encode(),
                      value=to_json_bytes({"musteri_id": musteri, "tur": tur}), on_delivery=cb)
    p.flush(15)
    for musteri, parts in sorted(goruldu.items()):
        log.info("musteri=%-3d -> partition(lar) %s", musteri, sorted(parts))
    assert all(len(s) == 1 for s in goruldu.values()), "Aynı key farklı partition'a gitti!"
    # Not: varsayılan partitioner librdkafka'da 'consistent_random' (murmur2 değil).
    # Java istemcisiyle AYNI partition'ı istiyorsanız: partitioner='murmur2_random'
    log.info("Doğrulandı: her key tek bir partition'a gitti.")


if __name__ == "__main__":
    main()
