"""
Ortak yardımcılar: konfigürasyon, logging, JSON (Decimal güvenli) serileştirme,
delivery callback, graceful shutdown, retry ve Dead Letter Topic yardımcıları.

Dosya: code/python/common.py
Bağımlılık: confluent-kafka==2.16.2 (yalnızca tip/sabitler için; bu modül broker'a bağlanmaz)

Tüm örnek script'ler bu modülü içe aktarır. Konfigürasyon ortam değişkenlerinden
okunur; böylece aynı kod dev / test / prod ortamlarında değişmeden çalışır.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import random
import signal
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Konfigürasyon
# ---------------------------------------------------------------------------
def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key)
    return v if v not in (None, "") else default


def base_config() -> dict[str, Any]:
    """Producer/Consumer/Admin için ortak bağlantı ayarları.

    Geliştirme:  KAFKA_BOOTSTRAP=localhost:9092 (varsayılan), PLAINTEXT
    Production:  KAFKA_SECURITY_PROTOCOL=SASL_SSL, KAFKA_SASL_MECHANISM=SCRAM-SHA-512,
                 KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD, KAFKA_SSL_CA_LOCATION
    """
    cfg: dict[str, Any] = {
        "bootstrap.servers": _env("KAFKA_BOOTSTRAP", "localhost:9092"),
        "client.id": _env("KAFKA_CLIENT_ID", "kafka-rehber"),
    }
    proto = _env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    cfg["security.protocol"] = proto
    if proto.startswith("SASL"):
        cfg["sasl.mechanism"] = _env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        cfg["sasl.username"] = _env("KAFKA_SASL_USERNAME")
        cfg["sasl.password"] = _env("KAFKA_SASL_PASSWORD")
    if proto.endswith("SSL"):
        ca = _env("KAFKA_SSL_CA_LOCATION")
        if ca:
            cfg["ssl.ca.location"] = ca
    return cfg


def producer_config(**overrides: Any) -> dict[str, Any]:
    cfg = base_config()
    cfg.update(
        {
            # Dayanıklılık: librdkafka'da enable.idempotence varsayılanı FALSE'tur
            # (Java istemcisinde 3.0'dan beri true). Açıkça açıyoruz.
            "enable.idempotence": True,
            "acks": "all",
            # Performans: küçük bir bekleme ile batch'leri doldur, sıkıştır.
            "linger.ms": 20,
            "batch.size": 131072,
            "compression.type": "zstd",
            # Teslim edilemeyen mesaj için üst süre (retry'lar dahil)
            "delivery.timeout.ms": 120000,
        }
    )
    if "transactional.id" in overrides:
        # librdkafka kuralı: message.timeout.ms (= delivery.timeout.ms) <= transaction.timeout.ms
        # Aksi halde Producer oluşturulurken _INVALID_ARG hatası alınır.
        # transaction.timeout.ms ayrıca broker'daki transaction.max.timeout.ms'i (varsayılan 15 dk) aşmamalı.
        cfg["transaction.timeout.ms"] = 120000
    cfg.update(overrides)
    return cfg


def consumer_config(group_id: str, **overrides: Any) -> dict[str, Any]:
    cfg = base_config()
    cfg.update(
        {
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # Offset'i işi bitirdikten SONRA biz commit edeceğiz (at-least-once)
            "enable.auto.commit": False,
            # Yeni nesil rebalance protokolü (KIP-848). Kafka 4.x broker ve
            # confluent-kafka >= 2.12 ile production-ready. Eski broker'da "classic" kullanın.
            "group.protocol": _env("KAFKA_GROUP_PROTOCOL", "consumer"),
        }
    )
    if cfg["group.protocol"] == "classic":
        cfg["partition.assignment.strategy"] = "cooperative-sticky"
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# JSON serileştirme (Decimal, datetime güvenli)
# ---------------------------------------------------------------------------
class FinansJSONEncoder(json.JSONEncoder):
    """Decimal -> string (hassasiyet kaybı yok), datetime -> ISO-8601 (UTC)."""

    def default(self, o: Any) -> Any:  # noqa: D401
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, datetime):
            if o.tzinfo is None:
                raise ValueError("Timezone'suz datetime serileştirilmez: %r" % o)
            return o.astimezone(timezone.utc).isoformat()
        if isinstance(o, date):
            return o.isoformat()
        return super().default(o)


def to_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, cls=FinansJSONEncoder, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def from_json_bytes(b: bytes | None) -> Any:
    if b is None:
        return None  # tombstone
    # parse_float=Decimal: "1250.75" gibi JSON sayıları float'a çevrilmeden okunur
    return json.loads(b.decode("utf-8"), parse_float=Decimal)


def decode_debezium_decimal(value_b64: str, scale: int) -> Decimal:
    """Debezium decimal.handling.mode=precise + JsonConverter çıktısını çözer.

    Değer: unscaled big-endian two's complement tamsayının base64 hali.
    Ölçek (scale) mesaj şemasındaki parameters.scale alanından gelir.
    """
    raw = base64.b64decode(value_b64)
    unscaled = int.from_bytes(raw, byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)


# ---------------------------------------------------------------------------
# Delivery report
# ---------------------------------------------------------------------------
def make_delivery_callback(log: logging.Logger) -> Callable:
    def _cb(err, msg):
        if err is not None:
            log.error("Teslim HATASI topic=%s key=%s err=%s", msg.topic(), msg.key(), err)
        else:
            log.debug(
                "Teslim edildi %s[%d]@%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), msg.key(),
            )

    return _cb


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
@dataclass
class ShutdownFlag:
    """SIGINT (Ctrl+C) ve SIGTERM (docker stop / k8s) sinyallerini yakalar."""

    running: bool = True
    _log: logging.Logger | None = field(default=None, repr=False)

    def install(self, log: logging.Logger | None = None) -> "ShutdownFlag":
        self._log = log
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)
        return self

    def _handler(self, signum, _frame):
        if self._log:
            self._log.info("Sinyal alındı (%s), döngü durduruluyor...", signal.Signals(signum).name)
        self.running = False


# ---------------------------------------------------------------------------
# Retry ve DLT
# ---------------------------------------------------------------------------
class RetriableError(Exception):
    """Geçici hata (ör. DB timeout). Tekrar denenir."""


class NonRetriableError(Exception):
    """Kalıcı hata (ör. bozuk JSON, iş kuralı ihlali). Doğrudan DLT'ye gider."""


def retry_call(
    fn: Callable[[], Any],
    *,
    attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    retry_on: tuple[type[BaseException], ...] = (RetriableError,),
    sleep: Callable[[float], None] = time.sleep,
    log: logging.Logger | None = None,
) -> Any:
    """Exponential backoff + jitter ile yeniden dener. Son denemede hatayı fırlatır."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if i == attempts:
                raise
            delay = min(max_delay, base_delay * (2 ** (i - 1))) * (0.5 + random.random() / 2)
            if log:
                log.warning("Deneme %d/%d başarısız (%s). %.2fs sonra tekrar.", i, attempts, exc, delay)
            sleep(delay)


def dlt_headers(msg, exc: BaseException, attempts: int) -> list[tuple[str, bytes]]:
    """DLT'ye gönderilen mesaja hata bağlamını header olarak ekler."""
    return [
        ("dlt.original.topic", msg.topic().encode()),
        ("dlt.original.partition", str(msg.partition()).encode()),
        ("dlt.original.offset", str(msg.offset()).encode()),
        ("dlt.exception.class", type(exc).__name__.encode()),
        ("dlt.exception.message", str(exc)[:1000].encode("utf-8", "replace")),
        ("dlt.attempts", str(attempts).encode()),
        ("dlt.failed.at", datetime.now(timezone.utc).isoformat().encode()),
    ]


def chunked(it: Iterable[Any], size: int) -> Iterable[list[Any]]:
    buf: list[Any] = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def safe_commit(consumer, log: logging.Logger | None = None, **kwargs) -> bool:
    """commit() commit edilecek offset yoksa _NO_OFFSET fırlatır; bunu sessizce yutar."""
    from confluent_kafka import KafkaError, KafkaException

    try:
        consumer.commit(asynchronous=False, **kwargs)
        return True
    except KafkaException as e:
        if e.args and e.args[0].code() == KafkaError._NO_OFFSET:
            return False
        if log:
            log.error("Commit hatası: %s", e)
        raise
