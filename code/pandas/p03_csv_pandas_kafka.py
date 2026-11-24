"""
CSV -> Pandas -> Kafka (temizlenmiş kayıtlar + reddedilenler ayrı topic'e)
Dosya: code/pandas/p03_csv_pandas_kafka.py
Çalıştırma: python p03_csv_pandas_kafka.py --dosya ../data/islemler_ornek.csv
Beklenen çıktı (örnek veriyle): "temiz=18 red=2 -> csv.yukleme / islemler.DLT"
Büyük dosyalar için chunksize ile parça parça okunur (bellek sabit kalır).
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import producer_config, setup_logging  # noqa: E402
from pandas_utils import df_to_kafka, temizle  # noqa: E402

log = setup_logging("p03")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dosya", required=True)
    ap.add_argument("--chunksize", type=int, default=50_000)
    a = ap.parse_args()
    p = Producer(producer_config())
    toplam_t = toplam_r = 0
    # dtype=str: pandas'ın tutarı float'a çevirip kuruş hassasiyetini bozmasını engeller
    for parca in pd.read_csv(a.dosya, dtype=str, keep_default_na=False, chunksize=a.chunksize):
        temiz, red = temizle(parca)
        toplam_t += df_to_kafka(temiz, p, "csv.yukleme", key_col="hareket_id")
        if not red.empty:
            toplam_r += df_to_kafka(red.astype(object).where(red.notna(), None), p, "islemler.DLT")
    log.info("temiz=%d red=%d -> csv.yukleme / islemler.DLT", toplam_t, toplam_r)


if __name__ == "__main__":
    main()
