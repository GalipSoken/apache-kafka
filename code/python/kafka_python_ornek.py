"""
kafka-python (saf Python istemci) ile aynı işin karşılaştırmalı örneği.
Dosya: code/python/kafka_python_ornek.py
Kurulum: pip install kafka-python==3.0.13
Çalıştırma: python kafka_python_ornek.py
"""
import json

from kafka import KafkaConsumer, KafkaProducer

BOOT = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOT,
    acks="all",
    enable_idempotence=True,
    linger_ms=20,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)
for i in range(5):
    fut = producer.send("islemler.json", key=str(i), value={"hareket_id": 90000 + i, "tutar": "10.00"})
    md = fut.get(timeout=10)                # senkron bekleme: RecordMetadata
    print("gönderildi", md.topic, md.partition, md.offset)
producer.flush()
producer.close()

consumer = KafkaConsumer(
    "islemler.json",
    bootstrap_servers=BOOT,
    group_id="rehber-kafka-python",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,               # 5 sn mesaj gelmezse iterasyon biter
    value_deserializer=lambda b: json.loads(b.decode()),
)
for msg in consumer:
    print(msg.partition, msg.offset, msg.value)
    consumer.commit()
consumer.close()
