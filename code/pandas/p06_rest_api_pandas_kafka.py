"""
REST API -> Pandas -> Kafka
Dosya: code/pandas/p06_rest_api_pandas_kafka.py
Çalıştırma:
  python p06_rest_api_pandas_kafka.py --url "https://api.frankfurter.app/latest?from=USD&to=TRY,EUR"
  # ağ yoksa:  python p06_rest_api_pandas_kafka.py --dosya ../data/ornek_kur.json
Beklenen çıktı: "2 kur kaydı kur.tcmb topic'ine gönderildi"
Not: Örnek URL herkese açık bir döviz API'sidir; kendi kaynağınızla (ör. TCMB EVDS) değiştirin.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import requests
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import producer_config, retry_call, RetriableError, setup_logging  # noqa: E402
from pandas_utils import df_to_kafka  # noqa: E402

log = setup_logging("p06")


def getir(url: str) -> dict:
    def _call():
        try:
            r = requests.get(url, timeout=10)
        except requests.RequestException as e:
            raise RetriableError(str(e)) from e
        if r.status_code >= 500 or r.status_code == 429:
            raise RetriableError(f"HTTP {r.status_code}")
        r.raise_for_status()
        return r.json(parse_float=Decimal)
    return retry_call(_call, attempts=4, base_delay=1, log=log)


def normalize(payload: dict) -> pd.DataFrame:
    """{'base':'USD','date':'2026-11-24','rates':{'TRY':..}} -> uzun format tablo"""
    df = pd.DataFrame([{"kaynak_para": payload["base"], "hedef_para": k, "kur": Decimal(str(v)),
                        "kur_tarihi": payload["date"]} for k, v in payload["rates"].items()])
    df["cekilme_zamani"] = pd.Timestamp(datetime.now(timezone.utc))
    df["anahtar"] = df["kaynak_para"] + df["hedef_para"] + "_" + df["kur_tarihi"]
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--url")
    g.add_argument("--dosya")
    a = ap.parse_args()
    payload = getir(a.url) if a.url else json.load(open(a.dosya), parse_float=Decimal)
    df = normalize(payload)
    n = df_to_kafka(df, Producer(producer_config()), "kur.tcmb", key_col="anahtar")
    log.info("%d kur kaydı kur.tcmb topic'ine gönderildi", n)


if __name__ == "__main__":
    main()
