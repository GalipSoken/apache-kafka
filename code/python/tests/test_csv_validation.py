import importlib
from decimal import Decimal

import pytest

csvmod = importlib.import_module("05_csv_to_kafka")


def test_virgullu_ondalik():
    r = csvmod.satir_dogrula({"hareket_id": "1", "musteri_id": "2", "islem_tipi": "EFT", "tutar": "12,50",
                              "para_birimi": "TRY", "islem_zamani": "2026-11-24T10:00:00+03:00"})
    assert r["tutar"] == Decimal("12.50")


def test_eksik_alan():
    with pytest.raises(ValueError, match="eksik"):
        csvmod.satir_dogrula({"hareket_id": "1", "musteri_id": "2", "islem_tipi": "EFT", "tutar": "",
                              "para_birimi": "TRY", "islem_zamani": "x"})
