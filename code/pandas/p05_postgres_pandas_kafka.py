"""
PostgreSQL -> Pandas -> Kafka (watermark/high-water-mark tabanlı artımlı okuma = POLLING)
Dosya: code/pandas/p05_postgres_pandas_kafka.py
Çalıştırma:
  export SRC_DSN="postgresql+psycopg://postgres:postgres@localhost:5433/bankdb"
  python p05_postgres_pandas_kafka.py --periyot 30
Sınırlamalar (neden CDC daha iyidir): DELETE'leri göremez, aynı saniyede gelen güncellemeleri
kaçırabilir, kaynağa sorgu yükü bindirir, ara durumları (A->B->C'de B'yi) kaçırır.
"""
import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import ShutdownFlag, producer_config, setup_logging  # noqa: E402
from pandas_utils import df_to_kafka  # noqa: E402

log = setup_logging("p05")
SQL = text("""
SELECT hareket_id, musteri_id, islem_tipi, tutar, para_birimi, islem_zamani
FROM public.hesap_hareket
WHERE hareket_id > :son_id
ORDER BY hareket_id
LIMIT :limit
""")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--periyot", type=int, default=30)
    ap.add_argument("--limit", type=int, default=10_000)
    a = ap.parse_args()
    eng = create_engine(os.getenv("SRC_DSN", "postgresql+psycopg://postgres:postgres@localhost:5433/bankdb"))
    p = Producer(producer_config())
    son_id = int(os.getenv("BASLANGIC_ID", "0"))   # prod'da bu değeri kalıcı bir yerde saklayın
    flag = ShutdownFlag().install(log)
    while flag.running:
        with eng.connect() as conn:
            # coerce_float=False: NUMERIC kolonlar Decimal olarak gelir (float'a çevrilmez)
            df = pd.read_sql(SQL, conn, params={"son_id": son_id, "limit": a.limit}, coerce_float=False)
        if not df.empty:
            n = df_to_kafka(df, p, "islemler.json", key_col="musteri_id")
            son_id = int(df["hareket_id"].max())
            log.info("%d satır gönderildi, yeni son_id=%d", n, son_id)
        time.sleep(a.periyot)


if __name__ == "__main__":
    main()
