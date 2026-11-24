"""
Kafka <-> Pandas yardımcıları (micro-batch işleme için).
Dosya: code/pandas/pandas_utils.py
Sürümler: pandas 3.1.1, SQLAlchemy 2.1.9, psycopg 3.3.9, confluent-kafka 2.16.2

Pandas bir streaming motoru DEĞİLDİR: burada Kafka'dan sınırlı büyüklükte bir mesaj
grubu (micro-batch) alınıp bellekte DataFrame'e çevrilir, işlenir ve sonuç yazılır.
"""
from __future__ import annotations

import json
import sys
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import FinansJSONEncoder  # noqa: E402

KURUS = Decimal("0.01")


# ---------------------------------------------------------------------------
# Kafka mesajları -> DataFrame
# ---------------------------------------------------------------------------
def messages_to_df(messages: Iterable[Any]) -> pd.DataFrame:
    """confluent_kafka.Message listesini DataFrame'e çevirir.
    Bozuk JSON'lar '_parse_error' kolonu ile işaretlenir (atılmaz, raporlanabilir)."""
    kayitlar = []
    for m in messages:
        if m.error() is not None or m.value() is None:
            continue
        meta = {"_topic": m.topic(), "_partition": m.partition(), "_offset": m.offset(),
                "_kafka_ts": m.timestamp()[1]}
        try:
            body = json.loads(m.value().decode("utf-8"), parse_float=Decimal)
            if not isinstance(body, dict):
                raise ValueError("JSON nesne değil")
            kayitlar.append({**body, **meta, "_parse_error": None})
        except (ValueError, UnicodeDecodeError) as e:
            kayitlar.append({**meta, "_parse_error": str(e)})
    return pd.DataFrame.from_records(kayitlar)


def records_to_df(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame.from_records(records)


# ---------------------------------------------------------------------------
# Temizleme ve tip dönüşümleri
# ---------------------------------------------------------------------------
def _to_decimal(x: Any) -> Decimal | None:
    if x is None or (isinstance(x, float) and pd.isna(x)) or x is pd.NA:
        return None
    try:
        return Decimal(str(x).replace(",", ".")).quantize(KURUS, rounding=ROUND_HALF_EVEN)
    except InvalidOperation:
        return None


def temizle(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(temiz_df, reddedilen_df) döndürür.

    Adımlar: parse hatası ayıkla -> zorunlu alan null kontrolü -> tip dönüşümü ->
    tarih/saat (UTC) -> duplicate (hareket_id) -> iş kuralı (tutar > 0).
    """
    if df.empty:
        return df, df
    df = df.copy()
    red = []

    if "_parse_error" in df:
        bozuk = df[df["_parse_error"].notna()].assign(_red_sebep="parse_error")
        red.append(bozuk)
        df = df[df["_parse_error"].isna()]

    zorunlu = ["hareket_id", "musteri_id", "islem_tipi", "tutar", "islem_zamani"]
    for k in zorunlu:
        if k not in df:
            df[k] = pd.NA
    eksik_maske = df[zorunlu].isna().any(axis=1) | (df[zorunlu].astype("string") == "").any(axis=1)
    red.append(df[eksik_maske].assign(_red_sebep="eksik_alan"))
    df = df[~eksik_maske]

    # Tipler: tamsayılar nullable Int64, tutar Decimal (object), metin 'string'
    df["hareket_id"] = pd.to_numeric(df["hareket_id"], errors="coerce").astype("Int64")
    df["musteri_id"] = pd.to_numeric(df["musteri_id"], errors="coerce").astype("Int64")
    df["islem_tipi"] = df["islem_tipi"].astype("string").str.upper().str.strip()
    df["tutar"] = df["tutar"].map(_to_decimal).astype(object)
    if "para_birimi" in df:
        df["para_birimi"] = df["para_birimi"].astype("string").fillna("TRY")
    else:
        df["para_birimi"] = "TRY"

    # Tarih/saat: her şeyi UTC'ye çevir; timezone'suz değerleri Europe/Istanbul kabul et
    ts = pd.to_datetime(df["islem_zamani"], errors="coerce", utc=False, format="ISO8601")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("Europe/Istanbul", nonexistent="shift_forward", ambiguous="NaT")
    df["islem_zamani"] = ts.dt.tz_convert("UTC")

    bozuk_tip = df["hareket_id"].isna() | df["tutar"].isna() | df["islem_zamani"].isna()
    red.append(df[bozuk_tip].assign(_red_sebep="tip_donusumu"))
    df = df[~bozuk_tip]

    # İş kuralı
    negatif = df["tutar"].map(lambda d: d <= 0)
    red.append(df[negatif].assign(_red_sebep="tutar_pozitif_degil"))
    df = df[~negatif]

    # Duplicate: aynı hareket_id birden çok kez gelirse EN SON offset'li kaydı tut
    sirala = ["hareket_id"] + (["_offset"] if "_offset" in df else [])
    df = df.sort_values(sirala)
    dup = df.duplicated(subset=["hareket_id"], keep="last")
    red.append(df[dup].assign(_red_sebep="duplicate"))
    df = df[~dup]

    red_df = pd.concat([r for r in red if not r.empty], ignore_index=True) if any(
        not r.empty for r in red) else pd.DataFrame(columns=list(df.columns) + ["_red_sebep"])
    return df.reset_index(drop=True), red_df


# ---------------------------------------------------------------------------
# Feature engineering ve aggregation
# ---------------------------------------------------------------------------
def feature_ekle(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    yerel = df["islem_zamani"].dt.tz_convert("Europe/Istanbul")
    df["saat"] = yerel.dt.hour
    df["hafta_sonu"] = yerel.dt.day_of_week >= 5
    df["mesai_disi"] = (df["saat"] < 9) | (df["saat"] >= 18)
    df["buyuk_islem"] = df["tutar"].map(lambda d: d >= Decimal("100000"))
    # müşteri bazında batch içi sıra ve kümülatif tutar
    df = df.sort_values(["musteri_id", "islem_zamani"])
    df["musteri_islem_sirasi"] = df.groupby("musteri_id").cumcount() + 1
    df["musteri_kumulatif_tutar"] = df.groupby("musteri_id")["tutar"].transform(
        lambda s: s.cumsum())
    return df.reset_index(drop=True)


def dakikalik_ozet(df: pd.DataFrame, pencere: str = "1min") -> pd.DataFrame:
    """Event-time'a göre tumbling pencere (pd.Grouper) + islem_tipi bazında özet.
    NOT: Bu sadece micro-batch İÇİNDEKİ veriyi özetler; batch'ler arası state tutmaz.
    Batch'ler arası doğru sonuç için hedefte upsert ile toplama (bkz. upsert SQL) gerekir."""
    if df.empty:
        return pd.DataFrame(columns=["pencere_baslangic", "islem_tipi", "islem_adedi",
                                     "toplam_tutar", "ortalama_tutar"])
    g = df.groupby([pd.Grouper(key="islem_zamani", freq=pencere), "islem_tipi"], observed=True)
    ozet = g.agg(islem_adedi=("hareket_id", "count"),
                 toplam_tutar=("tutar", lambda s: sum(s, Decimal("0")))).reset_index()
    ozet["ortalama_tutar"] = [
        (t / n).quantize(KURUS, rounding=ROUND_HALF_EVEN) for t, n in zip(ozet["toplam_tutar"], ozet["islem_adedi"])
    ]
    return ozet.rename(columns={"islem_zamani": "pencere_baslangic"})


# ---------------------------------------------------------------------------
# DataFrame -> Kafka
# ---------------------------------------------------------------------------
def df_to_kafka(df: pd.DataFrame, producer, topic: str, key_col: str | None = None) -> int:
    """Her satırı bir JSON olay olarak gönderir. Decimal ve Timestamp güvenli."""
    n = 0
    for rec in df.to_dict(orient="records"):
        temiz = {}
        for k, v in rec.items():
            if k.startswith("_"):
                continue
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime()
            elif v is pd.NA or (isinstance(v, float) and pd.isna(v)):
                v = None
            elif hasattr(v, "item") and not isinstance(v, (str, bytes, Decimal)):
                v = v.item()          # numpy skaler -> python
            temiz[k] = v
        key = str(temiz[key_col]).encode() if key_col else None
        while True:
            try:
                producer.produce(topic, key=key,
                                 value=json.dumps(temiz, cls=FinansJSONEncoder, ensure_ascii=False).encode())
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        n += 1
    producer.flush(30)
    return n


# ---------------------------------------------------------------------------
# DataFrame -> PostgreSQL (idempotent upsert)
# ---------------------------------------------------------------------------
OZET_UPSERT = """
INSERT INTO public.islem_ozet_dakika
  (pencere_baslangic, islem_tipi, islem_adedi, toplam_tutar, ortalama_tutar)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (pencere_baslangic, islem_tipi) DO UPDATE SET
  islem_adedi    = public.islem_ozet_dakika.islem_adedi + EXCLUDED.islem_adedi,
  toplam_tutar   = public.islem_ozet_dakika.toplam_tutar + EXCLUDED.toplam_tutar,
  ortalama_tutar = ROUND((public.islem_ozet_dakika.toplam_tutar + EXCLUDED.toplam_tutar)
                   / (public.islem_ozet_dakika.islem_adedi + EXCLUDED.islem_adedi), 2)
"""
# DİKKAT: Bu "toplayan" upsert idempotent DEĞİLDİR (aynı batch iki kez gelirse iki kez ekler).
# Exactly-once sonuç için ya ham kayıtları upsert edip özeti SQL ile hesaplayın (önerilen,
# bkz. p04), ya da işlenen (partition, offset) aralığını aynı DB transaction'ında saklayın.


def ozet_upsert(conn, ozet: pd.DataFrame) -> int:
    rows = [(r.pencere_baslangic.to_pydatetime(), r.islem_tipi, int(r.islem_adedi),
             r.toplam_tutar, r.ortalama_tutar) for r in ozet.itertuples(index=False)]
    if rows:
        with conn.cursor() as cur:
            cur.executemany(OZET_UPSERT, rows)
    return len(rows)
