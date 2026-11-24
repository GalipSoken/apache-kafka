"""
Avro + Schema Registry ile producer (Decimal ve timestamp mantıksal tipleri).
Dosya: code/python/avro_producer_schema_registry.py
Ön koşul: Schema Registry ->  docker compose -f code/docker/docker-compose.cdc.yml --profile avro up -d
Kurulum : pip install "confluent-kafka[avro,schemaregistry]==2.16.2"
Çalıştırma: SCHEMA_REGISTRY_URL=http://localhost:8081 python avro_producer_schema_registry.py
Beklenen: "10 Avro mesajı gönderildi (subject: islemler.avro-value)"
"""
import os
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from common import producer_config, setup_logging
from txn_data import sentetik_islem

log = setup_logging("avro-producer")
SCHEMA = (Path(__file__).resolve().parents[1] / "schemas" / "islem.v1.avsc").read_text()
TOPIC = "islemler.avro"


def main() -> None:
    sr = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    # auto.register.schemas=False production'da önerilir: şemalar CI/CD'de kaydedilir,
    # uygulama sadece kayıtlı şemayı kullanır.
    ser = AvroSerializer(sr, SCHEMA, conf={"auto.register.schemas": True})
    key_ser = StringSerializer("utf_8")
    p = Producer(producer_config())
    for i in range(1, 11):
        o = sentetik_islem(i)
        o.pop("islem_ts_ms")                 # şemada yok
        # Avro decimal -> Python Decimal, timestamp-micros -> timezone'lu datetime kabul edilir
        p.produce(TOPIC, key=key_ser(str(o["musteri_id"])),
                  value=ser(o, SerializationContext(TOPIC, MessageField.VALUE)))
        p.poll(0)
    p.flush(15)
    log.info("10 Avro mesajı gönderildi (subject: %s-value)", TOPIC)


if __name__ == "__main__":
    main()
