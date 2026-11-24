import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from confluent_kafka import Consumer, Producer

from common import (NonRetriableError, RetriableError, chunked, consumer_config,
                    decode_debezium_decimal, dlt_headers, from_json_bytes,
                    producer_config, retry_call, to_json_bytes)
from fakes import FakeMsg


def test_decimal_roundtrip_hassasiyet_kaybi_yok():
    o = {"tutar": Decimal("1500000.10"), "t": datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)}
    b = to_json_bytes(o)
    assert b'"tutar":"1500000.10"' in b
    back = from_json_bytes(b)
    assert Decimal(back["tutar"]) == Decimal("1500000.10")
    assert back["t"] == "2026-11-24T06:00:00+00:00"


def test_json_float_decimal_olarak_okunur():
    assert from_json_bytes(b'{"x": 0.1}')["x"] == Decimal("0.1")


def test_timezonesuz_datetime_reddedilir():
    with pytest.raises(ValueError):
        to_json_bytes({"t": datetime(2026, 1, 1)})


def test_tombstone_none():
    assert from_json_bytes(None) is None


@pytest.mark.parametrize("deger,scale,beklenen", [
    ("AeJA", 2, Decimal("1234.56")),     # 123456 -> 0x01E240
    ("/h2/", 2, Decimal("-1234.57")),    # negatif (two's complement)
    ("AA==", 2, Decimal("0.00")),
])
def test_debezium_decimal(deger, scale, beklenen):
    assert decode_debezium_decimal(deger, scale) == beklenen


def test_retry_basarili_olana_kadar_dener():
    sayac = {"n": 0}

    def fn():
        sayac["n"] += 1
        if sayac["n"] < 3:
            raise RetriableError("geçici")
        return "ok"

    assert retry_call(fn, attempts=5, sleep=lambda _s: None) == "ok"
    assert sayac["n"] == 3


def test_retry_kalici_hatayi_tekrar_denemez():
    sayac = {"n": 0}

    def fn():
        sayac["n"] += 1
        raise NonRetriableError("bozuk")

    with pytest.raises(NonRetriableError):
        retry_call(fn, attempts=5, sleep=lambda _s: None)
    assert sayac["n"] == 1


def test_retry_tukenince_firlatir():
    with pytest.raises(RetriableError):
        retry_call(lambda: (_ for _ in ()).throw(RetriableError("x")), attempts=3, sleep=lambda _s: None)


def test_dlt_headers():
    h = dict(dlt_headers(FakeMsg(b"x", "islemler.json", 2, 57), ValueError("boom"), 3))
    assert h["dlt.original.topic"] == b"islemler.json"
    assert h["dlt.original.partition"] == b"2"
    assert h["dlt.original.offset"] == b"57"
    assert h["dlt.exception.class"] == b"ValueError"


def test_chunked():
    assert list(chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]


def test_librdkafka_producer_config_gecerli():
    # librdkafka config'i nesne oluşturulurken doğrular; broker gerekmez.
    Producer(producer_config()).flush(0)
    Producer(producer_config(**{"transactional.id": "test-txn-1"}))


@pytest.mark.parametrize("proto", ["consumer", "classic"])
def test_librdkafka_consumer_config_gecerli(monkeypatch, proto):
    monkeypatch.setenv("KAFKA_GROUP_PROTOCOL", proto)
    c = Consumer(consumer_config("test-grup", **{"isolation.level": "read_committed"}))
    c.close()


def test_store_offsets_config_gecerli():
    Consumer(consumer_config("t", **{"enable.auto.commit": True, "enable.auto.offset.store": False})).close()
