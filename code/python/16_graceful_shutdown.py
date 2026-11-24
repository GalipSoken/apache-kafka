"""
16) Graceful shutdown: SIGTERM/SIGINT geldiğinde
    1) yeni mesaj almayı bırak, 2) eldeki işi bitir, 3) producer'ı flush et,
    4) offset'leri commit et, 5) consumer.close() ile gruptan düzgün ayrıl.
Dosya: code/python/16_graceful_shutdown.py
Çalıştırma: python 16_graceful_shutdown.py   ->  başka terminalde:  kill -TERM <pid>
Beklenen çıktı:
   Sinyal alındı (SIGTERM), döngü durduruluyor...
   Kapanış: producer flush kalan=0, son commit yapıldı, consumer kapatıldı
Kubernetes notu: terminationGracePeriodSeconds > (poll timeout + batch işleme süresi + flush süresi)
olmalı; aksi halde SIGKILL gelir ve son batch tekrar işlenir.
"""
import time

from confluent_kafka import Consumer, Producer

from common import (ShutdownFlag, consumer_config, from_json_bytes, producer_config,
                    safe_commit, setup_logging, to_json_bytes)

log = setup_logging("graceful")


def main() -> None:
    c = Consumer(consumer_config("rehber-graceful"))
    p = Producer(producer_config())
    flag = ShutdownFlag().install(log)

    def on_revoke(consumer, partitions):
        # Rebalance'ta partition elimizden alınmadan önce son işi commit et
        p.flush(10)
        safe_commit(consumer, log)
        log.info("revoke: %s", [x.partition for x in partitions])

    c.subscribe(["islemler.json"], on_revoke=on_revoke)
    islenen = 0
    try:
        while flag.running:
            msgs = c.consume(num_messages=100, timeout=1.0)
            for m in msgs:
                if m.error():
                    continue
                o = from_json_bytes(m.value())
                p.produce("islemler.sonuc", key=m.key(), value=to_json_bytes(o))
                islenen += 1
            p.poll(0)
            if msgs:
                p.flush(10)          # önce çıktı kalıcı olsun
                safe_commit(c, log)  # sonra girdi offset'i
    finally:
        t0 = time.monotonic()
        kalan = p.flush(15)
        safe_commit(c, log)
        c.close()
        log.info("Kapanış: producer flush kalan=%d, son commit yapıldı, consumer kapatıldı "
                 "(%d mesaj, %.1fs)", kalan, islenen, time.monotonic() - t0)


if __name__ == "__main__":
    main()
