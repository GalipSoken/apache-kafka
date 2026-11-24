from decimal import Decimal
from pathlib import Path

import pandas as pd

from fakes import FakeMsg, FakeProducer
from pandas_utils import dakikalik_ozet, df_to_kafka, feature_ekle, messages_to_df, temizle

DATA = Path(__file__).resolve().parents[2] / "data" / "islemler_ornek.csv"


def _msgs():
    return [
        FakeMsg(b'{"hareket_id":1,"musteri_id":5,"islem_tipi":"eft","tutar":"100.10","islem_zamani":"2026-11-24T09:00:10+03:00"}', offset=0),
        FakeMsg(b'{"hareket_id":2,"musteri_id":5,"islem_tipi":"EFT","tutar":"200.20","islem_zamani":"2026-11-24T09:00:50+03:00"}', offset=1),
        FakeMsg(b'{"hareket_id":2,"musteri_id":5,"islem_tipi":"EFT","tutar":"999.99","islem_zamani":"2026-11-24T09:00:50+03:00"}', offset=2),
        FakeMsg(b'bozuk{json', offset=3),
        FakeMsg(b'{"hareket_id":3,"musteri_id":6,"islem_tipi":"KART","tutar":"-5","islem_zamani":"2026-11-24T09:01:05+03:00"}', offset=4),
        FakeMsg(b'{"hareket_id":4,"musteri_id":6,"islem_tipi":"KART","islem_zamani":"2026-11-24T09:01:05+03:00"}', offset=5),
    ]


def test_messages_to_df_ve_temizle():
    df = messages_to_df(_msgs())
    assert len(df) == 6
    temiz, red = temizle(df)
    assert sorted(temiz["hareket_id"].tolist()) == [1, 2]
    # duplicate'te EN SON offset'li kayıt kalır
    assert temiz.loc[temiz.hareket_id == 2, "tutar"].iloc[0] == Decimal("999.99")
    assert set(red["_red_sebep"]) == {"parse_error", "duplicate", "tutar_pozitif_degil", "eksik_alan"}
    assert str(temiz["islem_zamani"].dt.tz) == "UTC"
    assert temiz["islem_tipi"].tolist() == ["EFT", "EFT"]


def test_ozet_decimal_toplam():
    temiz, _ = temizle(messages_to_df(_msgs()))
    oz = dakikalik_ozet(feature_ekle(temiz))
    assert len(oz) == 1
    r = oz.iloc[0]
    assert r.islem_adedi == 2
    assert r.toplam_tutar == Decimal("1100.09")
    assert r.ortalama_tutar == Decimal("550.04")    # banker's rounding (ROUND_HALF_EVEN)


def test_csv_ornek_dosya():
    df = pd.read_csv(DATA, dtype=str, keep_default_na=False)
    temiz, red = temizle(df)
    assert len(df) == 20
    assert len(temiz) == 18 and len(red) == 2
    assert Decimal("12.50") in set(temiz["tutar"])


def test_df_to_kafka_json():
    temiz, _ = temizle(messages_to_df(_msgs()))
    fp = FakeProducer()
    assert df_to_kafka(temiz, fp, "out", key_col="musteri_id") == 2
    topic, key, value, _ = fp.sent[0]
    assert topic == "out" and key == b"5"
    assert b'"tutar":"100.10"' in value.replace(b" ", b"")
    assert b"_offset" not in value
