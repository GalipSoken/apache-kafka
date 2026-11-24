# 4. Python ile Kafka Entegrasyonu

> **Bu bölümde:** `confluent-kafka` ve `kafka-python` karşılaştırması; 16 uygulamalı örnek (basit producer'dan transactional işlemciye); konfigürasyon yönetimi, logging, hata kontrolü ve testler.
>
> **Ön koşul:** [3. bölüm B](03-kurulum.md#b-docker-ile-kurulum-bu-rehberin-ana-ortamı) — tek broker ayakta ve topic'ler oluşturulmuş olmalı.
>
> **Sürümler:** Python 3.11+ (test: 3.13), `confluent-kafka==2.16.2` (librdkafka 2.16.0), `kafka-python==3.0.13`, `psycopg==3.3.9`.

---

## 4.1 Kütüphane karşılaştırması

| Kriter | `confluent-kafka` | `kafka-python` |
|---|---|---|
| Altyapı | **librdkafka** (C) üzerine ince Python katmanı | Saf Python |
| Performans | Çok yüksek; C seviyesinde batch/sıkıştırma | Orta; yüksek hacimde CPU darboğazı |
| Bakım | Confluent tarafından aktif, librdkafka ile eşzamanlı sürüm | Topluluk; 2024 sonrası yeniden aktifleşti (3.x hattı) |
| Idempotent producer | ✅ | ✅ (`enable_idempotence`) |
| Transactions (EOS) | ✅ | ⚠️ Sürümünüzde doğrulayın |
| KIP-848 consumer protokolü | ✅ (2.12+ production-ready) | ❌ / sınırlı |
| Schema Registry, Avro/Protobuf/JSON Schema serileştiriciler | ✅ (`confluent-kafka[avro,schemaregistry]`) | ❌ (harici) |
| AsyncIO | ✅ `AIOProducer` / `AIOConsumer` (yeni) | ❌ (aiokafka ayrı proje) |
| Kurulum | Wheel'lar librdkafka'yı içerir; ARM/Alpine'da derleme gerekebilir | `pip install`, bağımlılık yok |
| API stili | `produce()` + `poll()` + callback; `consume()` batch | `send()` → Future; consumer bir iterator |
| Ne zaman seçilir? | **Production, yüksek hacim, EOS, Schema Registry** | Basit script, saf Python zorunluluğu, öğrenme |

Bu rehberdeki tüm ana örnekler `confluent-kafka` kullanır. Karşılaştırma için aynı işin kafka-python sürümü: [`code/python/kafka_python_ornek.py`](../code/python/kafka_python_ornek.py).

Kurulum:
```bash
cd code/python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## 4.2 Proje yapısı ve çalıştırma sırası

```
code/python/
├── common.py                 # config, logging, JSON(Decimal), retry, DLT, shutdown, safe_commit
├── txn_data.py               # sentetik işlem verisi üreticisi
├── 00_admin_topics.py        # topic oluştur/listele
├── 01_producer_basic.py ... 16_graceful_shutdown.py
├── kafka_python_ornek.py
├── requirements.txt
└── tests/                    # pytest: birim + PostgreSQL entegrasyon
```

Önerilen sıra: `00` → `01`/`02` → `03`/`04` → `08` → `10` → `11` → `12`/`13` → `14` → `15` → `16`. Producer ve consumer'ları **iki ayrı terminalde** çalıştırın.

---

## 4.3 Konfigürasyon yönetimi

Tüm script'ler `common.py`'deki üç fonksiyonla yapılandırılır; değerler **ortam değişkenlerinden** gelir, böylece kod dev/test/prod'da değişmez:

| Ortam değişkeni | Varsayılan | Açıklama |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Virgülle ayrılmış broker listesi |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Prod: `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | — | Secret manager'dan enjekte edin |
| `KAFKA_SSL_CA_LOCATION` | — | Kurumsal CA PEM dosyası |
| `KAFKA_GROUP_PROTOCOL` | `consumer` | Eski broker'da `classic` |
| `LOG_LEVEL` | `INFO` | `DEBUG` ile her teslimat loglanır |

```python
# common.py (özet)
def producer_config(**overrides):
    cfg = base_config()
    cfg.update({
        "enable.idempotence": True,   # librdkafka'da varsayılan FALSE!
        "acks": "all",
        "linger.ms": 20, "batch.size": 131072, "compression.type": "zstd",
        "delivery.timeout.ms": 120000,
    })
    if "transactional.id" in overrides:
        cfg["transaction.timeout.ms"] = 120000   # bkz. 4.19 "Testlerin yakaladığı hatalar"
    cfg.update(overrides)
    return cfg
```

**Prod'a geçiş örneği:**
```bash
export KAFKA_BOOTSTRAP="kfk-1.bank.local:9092,kfk-2.bank.local:9092,kfk-3.bank.local:9092"
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_USERNAME=svc-odeme-producer
export KAFKA_SASL_PASSWORD="$(vault kv get -field=password secret/kafka/svc-odeme-producer)"
export KAFKA_SSL_CA_LOCATION=/etc/pki/bank-ca.pem
python 03_json_producer.py
```

---

## 4.4 Logging ve hata kontrolü ilkeleri

- `setup_logging(name)`: tek satır formatı (`zaman | seviye | bileşen | mesaj`), seviye `LOG_LEVEL`'dan.
- **Producer hataları iki yerde gelir:** (1) `produce()` anında senkron istisna (`BufferError` = yerel kuyruk dolu, `KafkaException` = geçersiz argüman); (2) **delivery callback**'te asenkron hata (`err` dolu). İkisini de ele alın.
- **Consumer hataları:** `msg.error()` kontrolü; `_PARTITION_EOF` hata değildir (bilgi amaçlı). Fatal hatalarda (`err.fatal()`) süreci yeniden başlatın.
- Kişisel veri (müşteri adı, TCKN, IBAN) **loglanmaz**; sadece ID ve offset loglayın (bkz. [13. bölüm KVKK](13-guvenlik.md)).

---

## 4.5 (1) Basit Producer — `01_producer_basic.py`

```python
p = Producer(producer_config())
for i in range(10):
    p.produce("islemler", value=f"merhaba kafka #{i}".encode(), on_delivery=cb)
    p.poll(0)          # delivery callback'leri tetikle (bloklamaz)
kalan = p.flush(15)    # hepsi teslim edilene kadar bekle
```
```bash
LOG_LEVEL=DEBUG python 01_producer_basic.py
# ... DEBUG | producer-basic | Teslim edildi islemler[4]@0 key=None
# ... INFO  | producer-basic | 10 mesaj gönderildi, kuyrukta kalan: 0
```
**Neden `poll()`?** librdkafka arka planda gönderir, ancak callback'ler ancak `poll()`/`flush()` çağrıldığında Python'da çalışır. Hiç `poll()` çağırmazsanız callback kuyruğu büyür.
**Olası hata:** `kalan > 0` → broker'a ulaşılamadı (`delivery.timeout.ms` içinde). Callback'te `_MSG_TIMED_OUT` görürsünüz.

## 4.6 (2) Basit Consumer — `02_consumer_basic.py`

```python
c = Consumer(consumer_config("rehber-basic", **{"enable.auto.commit": True}))
c.subscribe(["islemler"])
while flag.running:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error(): ...
    log.info("%s[%d]@%d value=%s", msg.topic(), msg.partition(), msg.offset(), msg.value().decode())
c.close()
```
`close()` çağrılmazsa grup, tüketicinin öldüğünü ancak `session.timeout.ms` (varsayılan 45 sn) sonra anlar → o süre boyunca partition'lar işlenmez.

## 4.7 (3) JSON Producer — `03_json_producer.py`

- `to_json_bytes()` → `Decimal` string olarak, `datetime` ISO-8601 UTC olarak yazılır. Timezone'suz `datetime` **reddedilir** (bankacılıkta "hangi saat dilimi?" belirsizliği hata kaynağıdır).
- `key=musteri_id` → aynı müşteri aynı partition → müşteri bazında sıralama.
- `timestamp=olay["islem_ts_ms"]` → Kafka record timestamp'i **event time** olur (ksqlDB/Flink pencereleri için).
- `BufferError` yakalanıp `poll(0.5)` ile kuyruk boşaltılır (yüksek hızda üretimde şart).

```bash
python 03_json_producer.py --adet 5000
# INFO | json-producer | 5000 işlem gönderildi (41250.3 msg/sn)
```
Örnek mesaj:
```json
{"hareket_id":1,"musteri_id":41,"islem_tipi":"ATM","tutar":"9850.15","para_birimi":"TRY",
 "islem_zamani":"2026-11-24T20:16:36.239004+00:00","islem_ts_ms":1795551396239,"kanal":"INTERNET"}
```

## 4.8 (4) JSON Consumer — `04_json_consumer.py`

`from_json_bytes()` `json.loads(..., parse_float=Decimal)` kullanır: JSON'da sayı olarak gelen `0.1` bile `Decimal("0.1")` olur. Header'lar `msg.headers()` ile `[(anahtar, bytes)]` listesi olarak okunur. Bozuk JSON'da örnek basitçe offset'i geçer; doğru yöntem DLT'dir (4.17).

## 4.9 (5) CSV → Kafka — `05_csv_to_kafka.py`

Satır satır akış halinde okur (bellek sabit), her satırı doğrular (`satir_dogrula`), virgüllü ondalığı (`12,50`) düzeltir, reddedilenleri loglar, kaynak dosya ve satır numarasını **header** olarak ekler (izlenebilirlik).
```bash
python 05_csv_to_kafka.py --dosya ../data/islemler_ornek.csv
# WARNING | csv-to-kafka | Satır 11 reddedildi: eksik alan: ['tutar']
# INFO    | csv-to-kafka | 19 satır gönderildi, 1 satır reddedildi
```
> Örnek CSV kasıtlı olarak bir virgüllü ondalık, bir eksik tutar ve bir duplicate `hareket_id` içerir. Bu saf Python sürümü duplicate'i yakalamaz (her satırı bağımsız işler); Pandas sürümü (`p03`) yakalar → 18 temiz, 2 red.

## 4.10 (6) Kafka → CSV — `06_kafka_to_csv.py`

Dosya rotasyonu (`--satir-limit`), **atomik yazım** (`.tmp` → `os.replace`), `fsync` ve **offset'i dosya kalıcı olduktan sonra commit**. Çökme anında son dosya tekrar yazılabilir (at-least-once); dosya adları zaman damgalı olduğu için üzerine yazma olmaz, aşağı akıştaki yükleyici `hareket_id` ile tekilleştirmelidir.

## 4.11 (7) Birden fazla topic dinleme — `07_multi_topic_consumer.py`

```python
c.subscribe(["islemler.json", "csv.yukleme", "kur.tcmb"], on_assign=on_assign)
# veya regex:   c.subscribe(["^islemler\\..*"])
```
Topic'e göre `HANDLERS` sözlüğüyle yönlendirme. Regex abonelikte desene uyan **yeni** topic'ler metadata yenilendiğinde otomatik eklenir. Dikkat: tek grup, farklı topic'lerdeki farklı işleme sürelerini paylaşır; biri yavaşsa diğerini de geciktirir → kritik akışları ayrı grup/sürece ayırın.

## 4.12 (8) Message Key kullanma — `08_message_key.py`

Her `musteri_id` için hangi partition'a gidildiğini toplar ve **tek partition** olduğunu `assert` ile doğrular.
```
INFO | message-key | musteri=3   -> partition(lar) [5]
INFO | message-key | musteri=7   -> partition(lar) [1]
INFO | message-key | Doğrulandı: her key tek bir partition'a gitti.
```

## 4.13 (9) Consumer Group ile paralel okuma — `09_consumer_group_parallel.py`

`multiprocessing` ile N süreç, aynı `group.id`. Python'da **thread değil süreç**: GIL nedeniyle CPU-yoğun işleme thread'lerle paralelleşmez; ayrıca her `Consumer` nesnesi tek thread'den kullanılmalıdır.
```bash
python 09_consumer_group_parallel.py --isci 3 --sure 30
# [isci-0] atandı: [0, 1]   [isci-1] atandı: [2, 3]   [isci-2] atandı: [4, 5]
python 09_consumer_group_parallel.py --isci 8     # 6 partition'lı topic'te 2 işçi boşta kalır
```
Ölçeklemenin iki yolu: (a) daha çok süreç/pod (partition sayısına kadar), (b) bir partition içindeki mesajları **key bazlı** iç kuyruklarla paralel işlemek (sıralama key bazında korunur; offset yönetimi karmaşıklaşır — Confluent'in "Parallel Consumer" deseni).

## 4.14 (10) Manuel offset commit — `10_manual_commit.py`

| Desen | Ayar | Ne zaman? |
|---|---|---|
| **A: senkron commit** | `enable.auto.commit=False`, her N mesajda `commit(asynchronous=False)` | Basit, net; commit gecikmesi throughput'u etkiler |
| **B: store + auto commit** | `enable.auto.commit=True`, `enable.auto.offset.store=False`, iş bitince `store_offsets(msg)` | **librdkafka'nın önerdiği yüksek throughput deseni**: commit arka planda, ama sadece "işlendi" işaretlenenler |
| Asenkron commit | `commit(asynchronous=True)` | Hızlı; hata callback ile gelir, sıra garantisi yoktur |

`safe_commit()`: commit edilecek offset yoksa librdkafka `_NO_OFFSET` fırlatır; yardımcı bunu sessizce yutar.

```bash
python 10_manual_commit.py --desen senkron --her 100
python 10_manual_commit.py --desen store
```

## 4.15 (11) Batch tüketimi — `11_batch_consume.py`

```python
msgs = c.consume(num_messages=500, timeout=2.0)    # tek çağrıda 500'e kadar mesaj
with conn.transaction():
    cur.executemany(UPSERT_SQL, rows)              # ON CONFLICT (hareket_id) DO UPDATE ...
safe_commit(c, log)                                # DB commit'ten SONRA Kafka commit
```
- **İdempotent upsert:** Aynı batch iki kez gelirse sonuç değişmez (testle doğrulandı: `test_batch_upsert_idempotent`).
- **Eski mesaj yeniyi ezmesin:** `WHERE islem_ham.kafka_offset <= EXCLUDED.kafka_offset` (test: `test_eski_offset_yeniyi_ezmez`).
- `max.poll.interval.ms`: batch işleme süresi bunu aşmamalı.

```bash
export PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"   # CDC compose'daki pg-target
python 11_batch_consume.py --batch 500
# INFO | batch-consume | 500 kayıt upsert edildi, offset commit edildi
```

## 4.16 (12) Retry mekanizması — `12_retry_consumer.py`

`retry_call()`: exponential backoff + jitter, sadece `RetriableError` için. `NonRetriableError` hiç tekrar denenmez (testle doğrulandı).

**Bloklayan retry'ın sınırı:** Retry sırasında `poll()` çağrılmaz. `deneme × max_bekleme` toplamı `max.poll.interval.ms`'i aşarsa tüketici gruptan atılır. Uzun süreli retry gerekiyorsa **retry topic** deseni:

```mermaid
flowchart LR
  A[islemler.json] --> C1[Ana consumer]
  C1 -- başarılı --> OK[(Hedef)]
  C1 -- geçici hata --> R1[islemler.retry.1m]
  R1 --> C2[Retry consumer<br/>1 dk bekletir]
  C2 -- başarılı --> OK
  C2 -- yine hata --> R2[islemler.retry.10m]
  R2 --> C3[Retry consumer<br/>10 dk]
  C3 -- tükendi --> DLT[islemler.DLT]
```
Bedeli: retry topic'e giden mesaj **sıralamadan çıkar** (aynı müşterinin sonraki işlemi önce işlenebilir). Sıralama kritikse (bakiye), bloklayan retry + alarm tercih edilir.

## 4.17 (13) Dead Letter Topic — `13_dead_letter_topic.py`

- Kalıcı hata (bozuk JSON, şema ihlali, iş kuralı) veya retry tükenmesi → mesaj **olduğu gibi** `islemler.DLT`'ye, hata bağlamı **header**'larda (`dlt.original.topic/partition/offset`, `dlt.exception.class/message`, `dlt.attempts`, `dlt.failed.at`).
- DLT'ye yazma `flush()` ile doğrulanmadan offset commit **edilmez**.
- Akış durmaz ("zehirli mesaj" — poison pill — tüm partition'ı kilitlemez).

Test:
```bash
echo 'bozuk{json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka:19092 --topic islemler.json
# WARNING | dlt | DLT'ye gönderildi islemler.json[2]@57 sebep=JSONDecodeError: Expecting value...
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
     --topic islemler.DLT --from-beginning --property print.headers=true
```
DLT operasyonu: DLT'nin de bir **sahibi**, alarmı (`DLT mesaj sayısı > 0`) ve yeniden işleme (redrive) prosedürü olmalıdır.

## 4.18 (14) Idempotent Producer — `14_idempotent_producer.py`

**Mekanizma:** Broker her producer'a bir **PID** verir; producer partition başına artan **sequence number** gönderir. Ağ hatasında aynı batch tekrar gelirse broker `(PID, partition, sequence)` ile tekrarı anlar ve yazmaz; sıra dışı gelen batch'i reddeder.

| Sağlar | Sağlamaz |
|---|---|
| Retry kaynaklı duplicate yok | Uygulama aynı olayı iki kez `produce()` ederse (ör. yeniden başlatma) duplicate'i önlemez |
| Partition içinde sıra korunur (in-flight ≤ 5) | Birden çok partition'a atomik yazma (→ transactions) |
| Tek producer oturumu | Producer yeniden başlarsa yeni PID → eski oturumla bağlantı yok |

Fatal hata (`err.fatal()`, ör. `OUT_OF_ORDER_SEQUENCE_NUMBER`) → producer nesnesi yeniden yaratılmalıdır.

## 4.19 (15) Transactional Producer — `15_transactional_processor.py`

**Consume → transform → produce** akışında Kafka içi exactly-once:
```python
p = Producer(producer_config(**{"transactional.id": f"rehber-txn-processor-{instance}"}))
p.init_transactions(30)
while running:
    msgs = c.consume(200, 1.0)
    p.begin_transaction()
    for m in msgs: p.produce("islemler.sonuc", key=m.key(), value=...)
    p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata(), 30)
    p.commit_transaction(30)
```
- Girdi offset'leri de transaction'ın parçası: **çıktılar + offset commit birlikte** görünür veya hiçbiri.
- Aşağı akıştaki tüketiciler `isolation.level=read_committed` ile okumalıdır.
- `transactional.id` instance başına **sabit ve benzersiz** (ör. StatefulSet pod adı). Aynı id ile yeni instance açılınca eskisi **fenced** olur (zombie koruması).
- Hata sınıflandırması: `txn_requires_abort()` → `abort_transaction()` + son commit'e `seek`; `retriable()` → tekrar dene; diğerleri fatal.
- **Kapsam:** PostgreSQL gibi dış sistemler bu transaction'a **dahil değildir.** DB'ye yazarken p04 desenini kullanın.

```bash
python 15_transactional_processor.py --instance 0
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic islemler.sonuc --from-beginning --isolation-level read_committed
```

**Testlerin yakaladığı hata (gerçek):** İlk sürümde `delivery.timeout.ms=120000` iken transactional producer oluşturmak şu hatayı verdi:
```
KafkaError{code=_INVALID_ARG,...,str="Failed to create producer: `message.timeout.ms` must be set <= `transaction.timeout.ms`"}
```
librdkafka'da `transaction.timeout.ms` varsayılanı 60 sn'dir. `common.producer_config()` artık transactional modda `transaction.timeout.ms=120000` ayarlar (broker'daki `transaction.max.timeout.ms` varsayılanı 15 dk'yı aşmamalı).

## 4.20 (16) Graceful shutdown — `16_graceful_shutdown.py`

```mermaid
sequenceDiagram
  participant OS as OS / Kubernetes
  participant App as Uygulama döngüsü
  participant P as Producer
  participant C as Consumer
  OS->>App: SIGTERM
  App->>App: flag.running = False (yeni batch alma)
  App->>App: eldeki batch'i bitir
  App->>P: flush(15)  (çıktılar kalıcı)
  App->>C: commit (girdi offset'leri)
  App->>C: close()  (LeaveGroup → hızlı rebalance)
  App-->>OS: exit 0
```
- `ShutdownFlag` SIGINT ve SIGTERM'i yakalar.
- `on_revoke` callback'i rebalance'ta partition elden çıkmadan önce flush + commit yapar.
- Kubernetes: `terminationGracePeriodSeconds` > poll timeout + batch işleme + flush süresi.

---

## 4.21 Testler

```bash
cd code/python
pytest -q tests                                   # broker GEREKMEZ (birim testler)
export TEST_PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"
pytest -q tests                                   # + PostgreSQL entegrasyon testleri
```

| Test dosyası | Ne doğrular |
|---|---|
| `test_common.py` | Decimal JSON roundtrip, timezone'suz tarih reddi, Debezium decimal çözümü, retry/kalıcı hata ayrımı, DLT header'ları, **librdkafka'nın producer/consumer/transactional config'lerini kabul etmesi** |
| `test_csv_validation.py` | Virgüllü ondalık, eksik alan |
| `test_pandas_utils.py` | Temizleme (parse hatası, duplicate, negatif tutar, eksik alan), UTC dönüşümü, Decimal toplam, banker's rounding, DataFrame→Kafka JSON |
| `test_postgres_integration.py` | Upsert idempotentliği, eski offset koruması, p04 özetinin yeniden hesaplanarak idempotent olması |

Broker gerektiren entegrasyon testleri için öneri: CI'da `apache/kafka-native` image'ı ile servis container'ı veya `testcontainers-python`.

**Bu rehberin doğrulama durumu:** Yukarıdaki 25 test, rehberin hazırlandığı ortamda (Python 3.13, confluent-kafka 2.16.2, PostgreSQL 16.15) **geçmiştir.** Kafka broker'a karşı uçtan uca çalıştırma bu ortamda yapılamamıştır (Docker registry erişimi yoktu); producer/consumer döngüleri resmi API'ye göre yazılmış, config'ler librdkafka tarafından doğrulanmıştır. Detay: [README → Doğrulama durumu](../README.md#doğrulama-durumu).

---

## 4.22 Sık hatalar (Python)

| Hata | Sebep | Çözüm |
|---|---|---|
| `BufferError: Local: Queue full` | Üretim hızı > gönderim hızı, `poll()` çağrılmıyor | Döngüde `poll(0)`; `BufferError`'da `poll(0.5)` ve tekrar dene; `queue.buffering.max.messages` |
| Callback'ler hiç çalışmıyor | `poll()`/`flush()` yok | Her produce sonrası `poll(0)` |
| `KafkaError._NO_OFFSET` | Commit edilecek offset yok | `safe_commit()` |
| `Application maximum poll interval exceeded` | İşleme `max.poll.interval.ms`'i aştı | Batch küçült, işi hızlandır, süreyi artır |
| Python ve Java producer aynı key'i farklı partition'a yazıyor | Farklı partitioner | Python'da `partitioner=murmur2_random` |
| `group.protocol=consumer` ile `session.timeout.ms` ayarlayınca hata | Yeni protokolde bu ayarlar broker'da | İstemciden kaldırın (`group.consumer.session.timeout.ms` broker tarafında) |
| Tutar `98000.00000000001` | `float` kullanımı | `Decimal` + `parse_float=Decimal` |

## Kaynaklar

- [confluent-kafka-python dokümantasyonu](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [confluent-kafka-python Changelog](https://docs.confluent.io/kafka-clients/python/current/changelog.html)
- [PyPI: confluent-kafka](https://pypi.org/project/confluent-kafka/)
- [librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [librdkafka INTRODUCTION.md — Exactly Once Semantics, Idempotent Producer](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md)
- [kafka-python dokümantasyonu](https://kafka-python.readthedocs.io/)
- [Apache Kafka — Transactions (Design)](https://kafka.apache.org/documentation/#semantics)
- [Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer)
