"""
Sentetik bankacılık işlemi üreticisi (tüm örneklerde ortak veri).
Dosya: code/python/txn_data.py
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

ISLEM_TIPLERI = ["EFT", "HAVALE", "KART", "ATM", "FAST"]


def sentetik_islem(hareket_id: int, simdi: datetime | None = None, rnd: random.Random | None = None) -> dict:
    r = rnd or random
    simdi = simdi or datetime.now(timezone.utc)
    tip = r.choice(ISLEM_TIPLERI)
    tutar = Decimal(r.randint(1_00, 250_000_00)) / 100      # kuruş hassasiyeti
    olay_zamani = simdi - timedelta(seconds=r.choice([0, 0, 0, 5, 30, 90]))
    return {
        "hareket_id": hareket_id,
        "musteri_id": r.randint(1, 50),
        "islem_tipi": tip,
        "tutar": tutar.quantize(Decimal("0.01")),
        "para_birimi": "TRY",
        # bazı olaylar geç gelsin (late-arriving) diye geçmişe kaydırılmış zaman
        "islem_zamani": olay_zamani,
        # Streaming SQL motorları (ksqlDB/Flink) için epoch milisaniye event-time alanı
        "islem_ts_ms": int(olay_zamani.timestamp() * 1000),
        "kanal": r.choice(["MOBIL", "INTERNET", "SUBE", "ATM"]),
    }
