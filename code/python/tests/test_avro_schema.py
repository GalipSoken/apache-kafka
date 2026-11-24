"""Avro şemaları: Decimal hassasiyeti ve şema evrimi (Schema Registry gerekmez, fastavro ile)."""
import io
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import fastavro

SCH = Path(__file__).resolve().parents[2] / "schemas"
v1 = fastavro.parse_schema(json.loads((SCH / "islem.v1.avsc").read_text()))
v2 = fastavro.parse_schema(json.loads((SCH / "islem.v2.avsc").read_text()))
KAYIT = {"hareket_id": 1, "musteri_id": 7, "islem_tipi": "EFT", "tutar": Decimal("1500000.10"),
         "para_birimi": "TRY", "islem_zamani": datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc), "kanal": None}


def _yaz(schema, rec):
    b = io.BytesIO(); fastavro.schemaless_writer(b, schema, rec); return b.getvalue()


def test_decimal_kesin_roundtrip():
    out = fastavro.schemaless_reader(io.BytesIO(_yaz(v1, KAYIT)), v1)
    assert out["tutar"] == Decimal("1500000.10")
    assert out["islem_zamani"] == KAYIT["islem_zamani"]


def test_backward_v2_okuyucu_v1_veriyi_okur():
    # Yeni consumer (v2 şeması) eski producer'ın (v1) verisini okuyabilmeli
    out = fastavro.schemaless_reader(io.BytesIO(_yaz(v1, KAYIT)), v1, v2)
    assert out["sube_kodu"] is None


def test_forward_v1_okuyucu_v2_veriyi_okur():
    # Eski consumer (v1) yeni producer'ın (v2) verisini okuyabilmeli (yeni alan yok sayılır)
    out = fastavro.schemaless_reader(io.BytesIO(_yaz(v2, {**KAYIT, "sube_kodu": "0123"})), v2, v1)
    assert "sube_kodu" not in out and out["tutar"] == Decimal("1500000.10")


def test_bilinmeyen_enum_default_a_duser():
    # Producer yeni bir enum değeri gönderirse eski okuyucu enum default'una düşer
    yeni = json.loads((SCH / "islem.v2.avsc").read_text())
    yeni["fields"][2]["type"]["symbols"].append("KRIPTO")
    yeni_s = fastavro.parse_schema(yeni)
    out = fastavro.schemaless_reader(io.BytesIO(_yaz(yeni_s, {**KAYIT, "islem_tipi": "KRIPTO", "sube_kodu": None})), yeni_s, v1)
    assert out["islem_tipi"] == "BILINMIYOR"
