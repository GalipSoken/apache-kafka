"""
DataFrame -> Kafka
Dosya: code/pandas/p01_dataframe_to_kafka.py
Çalıştırma: python p01_dataframe_to_kafka.py
Beklenen çıktı: "3 satır islemler.json topic'ine gönderildi"
"""
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import producer_config, setup_logging  # noqa: E402
from pandas_utils import df_to_kafka  # noqa: E402

log = setup_logging("p01")

df = pd.DataFrame({
    "hareket_id": [5001, 5002, 5003],
    "musteri_id": [1, 2, 1],
    "islem_tipi": ["EFT", "KART", "FAST"],
    "tutar": [Decimal("1500.00"), Decimal("89.90"), Decimal("250000.10")],
    "para_birimi": ["TRY"] * 3,
    "islem_zamani": pd.to_datetime(["2026-11-24 10:00", "2026-11-24 10:01", "2026-11-24 10:02"]).tz_localize("Europe/Istanbul"),
})
n = df_to_kafka(df, Producer(producer_config()), "islemler.json", key_col="musteri_id")
log.info("%d satır islemler.json topic'ine gönderildi", n)
