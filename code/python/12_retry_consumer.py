"""
12) Retry mekanizması: geçici hatalarda exponential backoff + jitter.
Dosya: code/python/12_retry_consumer.py
Çalıştırma: python 12_retry_consumer.py --hata-orani 0.3

Önemli: Bloklayan retry sırasında poll() çağrılmaz. Toplam bekleme max.poll.interval.ms'i
(varsayılan 300 sn) aşarsa consumer gruptan atılır ve rebalance olur. Bu yüzden:
  - deneme sayısı x max bekleme << max.poll.interval.ms olmalı
  - uzun süreli retry gerekiyorsa "retry topic" desenini kullanın (bkz. docs/04-python.md)
"""
import argparse
import random

from confluent_kafka import Consumer

from common import (RetriableError, ShutdownFlag, consumer_config, from_json_bytes,
                    retry_call, safe_commit, setup_logging)

log = setup_logging("retry-consumer")


def dis_servis_cagir(olay: dict, hata_orani: float) -> None:
    """Örn. fraud skoru servisi. Ara sıra timeout veriyormuş gibi davranır."""
    if random.random() < hata_orani:
        raise RetriableError("fraud-servisi timeout")
    log.info("işlendi hareket_id=%s", olay.get("hareket_id"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hata-orani", type=float, default=0.3)
    a = ap.parse_args()
    c = Consumer(consumer_config("rehber-retry"))
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            olay = from_json_bytes(msg.value())
            retry_call(lambda: dis_servis_cagir(olay, a.hata_orani),
                       attempts=5, base_delay=0.2, max_delay=5, log=log)
            safe_commit(c, log, message=msg)
    finally:
        c.close()


if __name__ == "__main__":
    main()
