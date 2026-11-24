"""
9) Consumer Group ile paralel okuma: N süreç, aynı group.id, partition'lar paylaşılır.
Dosya: code/python/09_consumer_group_parallel.py
Çalıştırma: python 09_consumer_group_parallel.py --isci 3 --sure 30
Beklenen çıktı: her işçi farklı partition kümesi alır:
   [isci-0] atandı: [0, 1]   [isci-1] atandı: [2, 3]   [isci-2] atandı: [4, 5]
Not: İşçi sayısı partition sayısından fazlaysa fazlalar BOŞTA bekler.
Not: Python GIL nedeniyle thread yerine ayrı süreç (multiprocessing) kullanılır.
"""
import argparse
import multiprocessing as mp
import time

from confluent_kafka import Consumer

from common import consumer_config, safe_commit, setup_logging


def isci(no: int, sure: int, topic: str) -> None:
    log = setup_logging(f"isci-{no}")
    c = Consumer(consumer_config("rehber-paralel", **{"client.id": f"isci-{no}"}))
    c.subscribe([topic],
                on_assign=lambda _c, ps: log.info("atandı: %s", sorted(p.partition for p in ps)),
                on_revoke=lambda _c, ps: log.info("geri alındı: %s", sorted(p.partition for p in ps)))
    sayac = 0
    bitis = time.time() + sure
    try:
        while time.time() < bitis:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            sayac += 1
            if sayac % 200 == 0:
                safe_commit(c, log)
        safe_commit(c, log)
    except Exception:  # noqa: BLE001
        log.exception("işçi hatası")
    finally:
        log.info("toplam işlenen: %d", sayac)
        c.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--isci", type=int, default=3)
    ap.add_argument("--sure", type=int, default=30)
    ap.add_argument("--topic", default="islemler.json")
    a = ap.parse_args()
    procs = [mp.Process(target=isci, args=(i, a.sure, a.topic)) for i in range(a.isci)]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join()


if __name__ == "__main__":
    main()
