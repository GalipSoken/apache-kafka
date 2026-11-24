"""
Kafka -> Pandas DataFrame (micro-batch) + temizleme + feature engineering + özet
Dosya: code/pandas/p02_kafka_to_dataframe.py
Çalıştırma: python p02_kafka_to_dataframe.py --batch 1000 --bekleme 5
Beklenen çıktı: her micro-batch için
   batch: 1000 mesaj -> 997 temiz, 3 red | red sebepleri: {'duplicate': 2, 'parse_error': 1}
   + dakikalık özet tablosu
"""
import argparse
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import ShutdownFlag, consumer_config, safe_commit, setup_logging  # noqa: E402
from pandas_utils import dakikalik_ozet, feature_ekle, messages_to_df, temizle  # noqa: E402

log = setup_logging("p02")


def micro_batch(c: Consumer, max_mesaj: int, max_sure: float) -> list:
    """Ya max_mesaj dolana ya da max_sure geçene kadar topla (boyut VEYA zaman tetiklemeli)."""
    toplanan, bitis = [], time.monotonic() + max_sure
    while len(toplanan) < max_mesaj and time.monotonic() < bitis:
        toplanan.extend(c.consume(num_messages=max_mesaj - len(toplanan), timeout=0.5))
    return toplanan


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--bekleme", type=float, default=5.0)
    a = ap.parse_args()
    c = Consumer(consumer_config("rehber-pandas"))
    c.subscribe(["islemler.json"])
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msgs = micro_batch(c, a.batch, a.bekleme)
            if not msgs:
                continue
            df = messages_to_df(msgs)
            temiz, red = temizle(df)
            log.info("batch: %d mesaj -> %d temiz, %d red | red sebepleri: %s",
                     len(msgs), len(temiz), len(red),
                     red["_red_sebep"].value_counts().to_dict() if not red.empty else {})
            if not temiz.empty:
                f = feature_ekle(temiz)
                log.info("mesai dışı işlem oranı: %.1f%%", 100 * f["mesai_disi"].mean())
                print(dakikalik_ozet(f).to_string(index=False))
            safe_commit(c, log)
    finally:
        c.close()


if __name__ == "__main__":
    main()
