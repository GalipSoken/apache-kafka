"""
1) Basit Producer: string mesaj gönderir.
Dosya: code/python/01_producer_basic.py
Çalıştırma: python 01_producer_basic.py
Beklenen çıktı: 10 satır "Teslim edildi islemler[p]@offset" (LOG_LEVEL=DEBUG ile) ve
               "10 mesaj gönderildi, kuyrukta kalan: 0"
"""
from confluent_kafka import Producer

from common import make_delivery_callback, producer_config, setup_logging

log = setup_logging("producer-basic")


def main() -> None:
    p = Producer(producer_config())
    cb = make_delivery_callback(log)
    for i in range(10):
        p.produce("islemler", value=f"merhaba kafka #{i}".encode(), on_delivery=cb)
        # poll(0): kuyruktaki delivery callback'lerini tetikler, bloklamaz
        p.poll(0)
    # flush: tüm mesajlar teslim edilene (veya timeout'a) kadar bekler
    kalan = p.flush(15)
    log.info("10 mesaj gönderildi, kuyrukta kalan: %d", kalan)


if __name__ == "__main__":
    main()
