"""
PostgreSQL entegrasyon testleri (Kafka broker GEREKMEZ, PostgreSQL gerekir).
Çalıştırma:
  export TEST_PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"   # target_init.sql uygulanmış DB
  pytest -q tests/test_postgres_integration.py
"""
import importlib
import os
from decimal import Decimal

import psycopg
import pytest

from fakes import FakeMsg
from pandas_utils import messages_to_df, temizle

DSN = os.getenv("TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_PG_DSN tanımlı değil")

batch = importlib.import_module("11_batch_consume")
p04 = importlib.import_module("p04_kafka_pandas_postgres")


@pytest.fixture()
def conn():
    with psycopg.connect(DSN) as c:
        with c.cursor() as cur:
            cur.execute("TRUNCATE public.islem_ham, public.islem_ozet_dakika, public.kafka_offsetleri")
        c.commit()
        yield c


def _m(hid, tutar, off, dk="00"):
    return FakeMsg(('{"hareket_id":%d,"musteri_id":1,"islem_tipi":"EFT","tutar":"%s","para_birimi":"TRY",'
                    '"islem_zamani":"2026-11-24T09:%s:10+03:00"}' % (hid, tutar, dk)).encode(), offset=off)


def test_batch_upsert_idempotent(conn):
    msgs = [_m(1, "10.10", 0), _m(2, "20.20", 1)]
    for _ in range(2):                       # aynı batch iki kez (çökme sonrası tekrar) gelsin
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(batch.UPSERT_SQL, list(batch.satirlar(msgs)))
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), sum(tutar) FROM public.islem_ham")
        assert cur.fetchone() == (2, Decimal("30.30"))


def test_eski_offset_yeniyi_ezmez(conn):
    with conn.cursor() as cur:
        cur.executemany(batch.UPSERT_SQL, list(batch.satirlar([_m(1, "99.99", 10)])))
        cur.executemany(batch.UPSERT_SQL, list(batch.satirlar([_m(1, "11.11", 3)])))  # geç gelen eski mesaj
        cur.execute("SELECT tutar, kafka_offset FROM public.islem_ham WHERE hareket_id=1")
        assert cur.fetchone() == (Decimal("99.99"), 10)
    conn.commit()


def test_p04_ozet_yeniden_hesap_idempotent(conn):
    temiz, _ = temizle(messages_to_df([_m(1, "100.00", 0), _m(2, "50.01", 1), _m(3, "7.00", 2, dk="01")]))
    for _ in range(2):
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(p04.HAM_UPSERT, [
                    (int(r.hareket_id), int(r.musteri_id), r.islem_tipi, r.tutar, r.para_birimi,
                     r.islem_zamani.to_pydatetime(), r.k_topic, int(r.k_partition), int(r.k_offset))
                    for r in temiz.rename(columns=p04.META_ADLARI).itertuples(index=False)])
                pencereler = sorted({t.floor("min").to_pydatetime() for t in temiz["islem_zamani"]})
                cur.execute(p04.OZET_YENIDEN, (pencereler,))
                cur.executemany(p04.OFFSET_UPSERT, [(p04.GROUP, p04.TOPIC, 0, 3)])
    with conn.cursor() as cur:
        cur.execute("SELECT islem_adedi, toplam_tutar, ortalama_tutar FROM public.islem_ozet_dakika "
                    "ORDER BY pencere_baslangic")
        assert cur.fetchall() == [(2, Decimal("150.01"), Decimal("75.01")), (1, Decimal("7.00"), Decimal("7.00"))]
        cur.execute("SELECT sonraki_offset FROM public.kafka_offsetleri")
        assert cur.fetchone() == (3,)
