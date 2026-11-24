"""
10) Manuel offset commit: iki desen.
Dosya: code/python/10_manual_commit.py
Çalıştırma: python 10_manual_commit.py --desen senkron|store

Desen A (senkron): Her N mesajda bir commit(asynchronous=False). Basit, at-least-once.
Desen B (store):  enable.auto.commit=True + enable.auto.offset.store=False.
                  İş bitince store_offsets(msg); arka plan thread'i periyodik commit eder.
                  Yüksek throughput'ta önerilen librdkafka desenidir.

Offset commit edilmezse: consumer yeniden başladığında SON COMMIT EDİLEN offset'ten
devam eder -> aradaki mesajlar TEKRAR işlenir (duplicate). Bu yüzden işlem idempotent olmalı.
"""
import argparse

from confluent_kafka import Consumer

from common import ShutdownFlag, consumer_config, from_json_bytes, safe_commit, setup_logging

log = setup_logging("manual-commit")


def isle(olay: dict) -> None:
    # İş mantığı (DB'ye yaz, API çağır...). Hata fırlatırsa offset İLERLEMEZ.
    if olay.get("tutar") is None:
        raise ValueError("tutar yok")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--desen", choices=["senkron", "store"], default="senkron")
    ap.add_argument("--her", type=int, default=100, help="senkron desende kaç mesajda bir commit")
    a = ap.parse_args()

    if a.desen == "senkron":
        cfg = consumer_config("rehber-manual-senkron")
    else:
        cfg = consumer_config("rehber-manual-store", **{
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "auto.commit.interval.ms": 5000,
        })
    c = Consumer(cfg)
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    n = 0
    try:
        while flag.running:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            isle(from_json_bytes(msg.value()))
            n += 1
            if a.desen == "senkron":
                if n % a.her == 0:
                    safe_commit(c, log)
                    log.info("%d mesaj sonrası commit edildi", n)
            else:
                c.store_offsets(message=msg)   # "bu mesaj işlendi" işareti
    finally:
        if a.desen == "senkron":
            safe_commit(c, log)
        c.close()   # store deseninde close() saklanan son offset'leri commit eder


if __name__ == "__main__":
    main()
