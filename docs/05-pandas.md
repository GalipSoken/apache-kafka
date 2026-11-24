# 5. Pandas ile Kafka Veri İşleme

> **Bu bölümde:** DataFrame ↔ Kafka, CSV/PostgreSQL/REST API ↔ Pandas ↔ Kafka, çoklu mesajdan DataFrame, temizleme (null, duplicate, tip, tarih/saat), feature engineering, aggregation, zaman bazlı analiz, micro-batch; ve en önemlisi **Pandas'ın sınırları** ile Spark/Flink'e geçiş kriterleri.
>
> **Kod:** [`code/pandas/`](../code/pandas/) — `pandas_utils.py` (ortak fonksiyonlar), `p01`–`p06` (senaryolar).
> **Sürümler:** pandas 3.1.1, SQLAlchemy 2.1.9, psycopg 3.3.9.

---

## 5.0 Önce doğru zihinsel model: Kafka ve Pandas **aynı işi yapmaz**

| | Kafka | Pandas |
|---|---|---|
| Ne yapar | Olayları **taşır ve saklar** | Bellekteki tabloyu **analiz eder** |
| Veri | Sınırsız akış (unbounded) | Sınırlı tablo (bounded), RAM'e sığmalı |
| Dağıtık mı | Evet (cluster) | Hayır (tek süreç, büyük ölçüde tek çekirdek) |
| Durum (state) | Log + offset | Python değişkeni (süreç ölünce gider) |
| Hata toleransı | Replikasyon | Yok (sizin kodunuz) |

**Doğru kullanım:** Pandas'ı bir Kafka **consumer'ının içinde**, belirli büyüklükteki **mesaj gruplarını (micro-batch)** işlemek için kullanırsınız. Kafka'nın yerini almaz, Kafka da Pandas'ın.

```mermaid
flowchart LR
  K[(Kafka topic)] -->|"consume(num_messages=1000, timeout)"| MB[Micro-batch<br/>list of Message]
  MB --> DF[messages_to_df<br/>DataFrame]
  DF --> CL[temizle<br/>null/dup/tip/tz]
  CL --> FE[feature_ekle]
  FE --> AG[dakikalik_ozet<br/>groupby]
  AG --> OUT1[(PostgreSQL<br/>upsert)]
  AG --> OUT2[(Kafka<br/>sonuç topic)]
  OUT1 --> CM[offset commit]
```

---

## 5.1 Pandas 3.x notları (2026)

- **Copy-on-Write** varsayılan: zincirleme atama (`df[df.x>0]["y"] = 1`) artık orijinali değiştirmez; `df.loc[...]` kullanın.
- **Varsayılan string tipi** `str` (PyArrow kuruluysa Arrow destekli); metin kolonlarında `astype("string")` ile açık olun.
- Bazı eski takma adlar kaldırılıyor (ör. `Series.dt.dayofweek` yerine `dt.day_of_week` — testlerimiz `Pandas4Warning` ile bunu yakaladı).
- **Decimal:** Pandas'ın yerleşik decimal dtype'ı yoktur. İki seçenek:
  - `object` kolonunda Python `Decimal` (bu rehber) → kesin, ama yavaş (Python seviyesinde döngü).
  - `pd.ArrowDtype(pa.decimal128(18, 2))` → hızlı ve kesin; PyArrow gerekir.
  - **Asla** `float64` (bkz. 10. bölüm).

---

## 5.2 Pandas DataFrame → Kafka (`p01_dataframe_to_kafka.py`)

```python
n = df_to_kafka(df, Producer(producer_config()), "islemler.json", key_col="musteri_id")
```
`df_to_kafka()` her satırı JSON olaya çevirir: `pd.Timestamp` → timezone'lu `datetime` → ISO-8601 UTC; `pd.NA`/`NaN` → `null`; NumPy skalerleri → Python tipleri; `_` ile başlayan meta kolonlar gönderilmez; `BufferError`'da `poll()` ile bekler.

```bash
cd code/pandas && python p01_dataframe_to_kafka.py
# INFO | p01 | 3 satır islemler.json topic'ine gönderildi
```

## 5.3 Kafka → Pandas DataFrame (`p02_kafka_to_dataframe.py`)

```python
def micro_batch(c, max_mesaj, max_sure):
    toplanan, bitis = [], time.monotonic() + max_sure
    while len(toplanan) < max_mesaj and time.monotonic() < bitis:
        toplanan.extend(c.consume(num_messages=max_mesaj - len(toplanan), timeout=0.5))
    return toplanan
```
Micro-batch **boyut VEYA zaman** dolunca tetiklenir; böylece düşük trafikte de sonuç gecikmez.

```bash
python p02_kafka_to_dataframe.py --batch 1000 --bekleme 5
# INFO | p02 | batch: 1000 mesaj -> 997 temiz, 3 red | red sebepleri: {'duplicate': 2, 'parse_error': 1}
# INFO | p02 | mesai dışı işlem oranı: 41.3%
#       pencere_baslangic islem_tipi  islem_adedi toplam_tutar ortalama_tutar
# 2026-11-24 20:15:00+00:00        ATM          203  25123456.12      123760.87
```

## 5.4 Çoklu mesajlardan DataFrame oluşturma (`messages_to_df`)

```python
kayitlar.append({**body, "_topic": m.topic(), "_partition": m.partition(),
                 "_offset": m.offset(), "_kafka_ts": m.timestamp()[1], "_parse_error": None})
df = pd.DataFrame.from_records(kayitlar)
```
- Bozuk mesajlar **atılmaz**, `_parse_error` ile işaretlenir → raporlanabilir/DLT'ye gidebilir.
- Kafka meta verisi (`_topic`, `_partition`, `_offset`) korunur: duplicate çözümü ve offset yönetimi için gerekli.
- Performans: `from_records` tek seferde liste → DataFrame; döngü içinde `pd.concat` **yapmayın** (O(n²)).

> **Testlerin yakaladığı tuzak:** `DataFrame.itertuples()` alt çizgiyle başlayan kolon adlarını (`_offset`) konumsal adlara (`_6`) çevirir; `r._offset` → `AttributeError`. `p04` meta kolonları `itertuples` öncesi `k_offset` gibi adlara çevirir (`META_ADLARI`).

## 5.5 Veri temizleme (`temizle`) — sırasıyla

| Adım | Kod | Reddedilen kayıt sebebi |
|---|---|---|
| 1. Parse hatası | `_parse_error.notna()` | `parse_error` |
| 2. Null / boş zorunlu alan | `df[zorunlu].isna() \| == ""` | `eksik_alan` |
| 3. Tip dönüşümü | `pd.to_numeric(..., errors="coerce").astype("Int64")`, `Decimal(str(x).replace(",", "."))` | `tip_donusumu` |
| 4. Tarih/saat | `pd.to_datetime(..., format="ISO8601")` → TZ yoksa `Europe/Istanbul` kabul → **UTC** | `tip_donusumu` |
| 5. İş kuralı | `tutar > 0` | `tutar_pozitif_degil` |
| 6. Duplicate | `hareket_id` bazında **en büyük offset'li** kaydı tut | `duplicate` |

Fonksiyon `(temiz_df, reddedilen_df)` döndürür. Testle doğrulanan davranış (`test_messages_to_df_ve_temizle`): 6 mesajdan 2 temiz; red sebepleri `{parse_error, duplicate, tutar_pozitif_degil, eksik_alan}`; duplicate'te **son** gelen değer (`999.99`) kalır.

### Null değer kontrolü — ayrıntı
- `pd.NA` (nullable tipler), `None`, `NaN` ve boş string `""` farklı şeylerdir. CSV'yi `dtype=str, keep_default_na=False` ile okursanız boş hücre `""` gelir (pandas'ın "NA" yazısını null sanmasını da engeller).
- Zorunlu olmayan alanlar için varsayılan (`para_birimi` → `TRY`), zorunlular için red.
- Raporlama: `df.isna().mean()` ile kolon bazında null oranı → veri kalitesi metriği.

### Duplicate kayıtların yönetimi
| Kaynak | Çözüm |
|---|---|
| Producer retry | Idempotent producer (broker tarafında) |
| Consumer yeniden işleme (at-least-once) | Batch içinde `drop_duplicates` + hedefte `ON CONFLICT` upsert |
| Kaynak sistem aynı olayı iki kez üretti | İş anahtarı (`hareket_id`) ile tekilleştirme; batch'ler arası için hedef tablo unique key |

### Veri tipi dönüşümleri
| Kaynak tipi | Pandas tipi (önerilen) | Neden |
|---|---|---|
| Tamsayı ID | `Int64` (nullable) | `int64` null kabul etmez, `float64`'e kayar |
| Tutar | `object[Decimal]` veya `ArrowDtype(decimal128)` | Kesinlik |
| Kod/kategori | `string` veya `category` | Bellek ve karşılaştırma tutarlılığı |
| Zaman | `datetime64[ns, UTC]` | Tek saat dilimi |
| Bayrak | `boolean` (nullable) | |

### Tarih ve saat dönüşümleri
- **Kural:** İçeride her şey **UTC**; kullanıcıya gösterirken `tz_convert("Europe/Istanbul")`.
- Türkiye 2016'dan beri sabit UTC+3 kullanır (yaz saati yok), ama eski verilerde DST vardır → `nonexistent`/`ambiguous` parametreleri.
- Epoch milisaniye → `pd.to_datetime(df["islem_ts_ms"], unit="ms", utc=True)`.
- Kafka record timestamp'i (`_kafka_ts`) ile olay zamanı (`islem_zamani`) farkı = **ingestion gecikmesi** → izlenecek bir metrik.

## 5.6 Feature engineering (`feature_ekle`)

```python
yerel = df["islem_zamani"].dt.tz_convert("Europe/Istanbul")
df["saat"] = yerel.dt.hour
df["hafta_sonu"] = yerel.dt.day_of_week >= 5
df["mesai_disi"] = (df["saat"] < 9) | (df["saat"] >= 18)
df["buyuk_islem"] = df["tutar"].map(lambda d: d >= Decimal("100000"))
df["musteri_islem_sirasi"] = df.groupby("musteri_id").cumcount() + 1
df["musteri_kumulatif_tutar"] = df.groupby("musteri_id")["tutar"].transform(lambda s: s.cumsum())
```

> **Kritik sınır:** Bu özellikler **sadece batch içindeki** veriye bakar. "Müşterinin son 24 saatteki toplam EFT'si" gibi bir özellik batch'ler arası **durum (state)** ister. Pandas'ta bunu ancak dış bir state deposuyla (Redis, PostgreSQL) yapabilirsiniz; bu noktada Flink/Kafka Streams doğru araçtır.

## 5.7 Aggregation, GroupBy ve zaman bazlı analiz (`dakikalik_ozet`)

```python
g = df.groupby([pd.Grouper(key="islem_zamani", freq="1min"), "islem_tipi"], observed=True)
ozet = g.agg(islem_adedi=("hareket_id", "count"),
             toplam_tutar=("tutar", lambda s: sum(s, Decimal("0"))))
ozet["ortalama_tutar"] = [(t / n).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN) ...]
```
- `pd.Grouper(freq="1min")` = event-time **tumbling pencere** (batch içinde).
- Decimal toplama `sum(s, Decimal("0"))`; ortalama banker's rounding ile (test: `(100.00 + 999.99)/2 = 550.045 → 550.04`).
- Diğer zaman analizleri: `df.set_index("islem_zamani").resample("5min")`, kayan pencere `rolling("10min", on="islem_zamani")` — yine **batch içinde**.

**Batch'ler arası doğru toplam:** Aynı dakika iki batch'e bölünebilir. İki seçenek:
1. **Toplayan upsert** (`pandas_utils.OZET_UPSERT`): `adet = eski + yeni`. Basit ama **idempotent değil** (batch tekrar gelirse iki kez ekler).
2. **Ham upsert + özeti SQL ile yeniden hesaplama** (`p04.OZET_YENIDEN`): etkilenen pencereler ham tablodan yeniden hesaplanır → **idempotent** (test: `test_p04_ozet_yeniden_hesap_idempotent`). **Önerilen.**

## 5.8 CSV → Pandas → Kafka (`p03_csv_pandas_kafka.py`)

```python
for parca in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=50_000):
    temiz, red = temizle(parca)
    df_to_kafka(temiz, p, "csv.yukleme", key_col="hareket_id")
    df_to_kafka(red, p, "islemler.DLT")
```
- `dtype=str`: pandas'ın tutarı `float`'a çevirip kuruşu bozmasını engeller.
- `chunksize`: GB'lık dosyada bellek sabit kalır.
- Örnek veriyle (test: `test_csv_ornek_dosya`): 20 satır → **18 temiz, 2 red** (eksik tutar + duplicate).

## 5.9 Kafka → Pandas → PostgreSQL (`p04_kafka_pandas_postgres.py`) — uçtan uca exactly-once

```mermaid
sequenceDiagram
  participant K as Kafka
  participant C as Consumer + Pandas
  participant PG as PostgreSQL
  C->>PG: on_assign: SELECT sonraki_offset FROM kafka_offsetleri
  C->>K: seek(partition, sonraki_offset)
  loop her micro-batch
    K-->>C: 1000 mesaj
    C->>C: messages_to_df → temizle
    C->>PG: BEGIN
    C->>PG: islem_ham UPSERT
    C->>PG: islem_ozet_dakika yeniden hesap
    C->>PG: kafka_offsetleri UPSERT (son offset + 1)
    C->>PG: COMMIT
    C-->>K: (bilgi amaçlı) async commit
  end
```
- Offset'ler **veriyle aynı transaction'da** PostgreSQL'de. Çökme nerede olursa olsun ya hepsi yazılır ya hiçbiri.
- `on_assign` callback'inde partition offset'leri DB'den okunup listeye yazılır; confluent-kafka callback dönünce protokole uygun atamayı kendisi yapar.
- Kafka'ya da commit yapılır ki lag izleme araçları (kafka-exporter) doğru lag görsün; **doğruluk kaynağı DB'dir.**

```bash
export PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"
python p04_kafka_pandas_postgres.py
# INFO | p04 | assign + DB offset'leri: [(0, -2), (1, -2), ...]     (-2 = OFFSET_BEGINNING)
# INFO | p04 | yazıldı: temiz=998 red=2 offsetler={0: 166, 1: 171, ...}
```

## 5.10 PostgreSQL → Pandas → Kafka (`p05_postgres_pandas_kafka.py`)

```python
df = pd.read_sql(SQL, conn, params={"son_id": son_id, "limit": 10_000}, coerce_float=False)
```
`coerce_float=False` → `NUMERIC` kolonlar `Decimal` gelir. Bu bir **polling** (sorgulama) yaklaşımıdır:

| Polling'in eksikleri | CDC (Debezium) ile |
|---|---|
| DELETE'leri göremez | Görür (`op: d` + tombstone) |
| Ara güncellemeleri kaçırır (A→B→C'de B) | Her değişikliği görür |
| `updated_at` kolonu ve indeks ister | Şema değişikliği gerekmez |
| Kaynağa periyodik sorgu yükü | WAL okur, sorgu yükü yok |
| Saat kayması / aynı zaman damgası sorunları | LSN sıralı |

Polling ne zaman yeterli: küçük tablolar, dakikalık gecikme kabul, DELETE yok, DB'de replikasyon yetkisi alınamıyor. Aksi halde [6. bölüm — CDC](06-cdc-debezium.md).

## 5.11 REST API → Pandas → Kafka (`p06_rest_api_pandas_kafka.py`)

- `retry_call` ile 5xx/429/timeout'ta yeniden deneme, 4xx'te hemen hata.
- `r.json(parse_float=Decimal)` → kur değerleri kesin.
- `normalize()` iç içe JSON'u uzun formata çevirir; `anahtar = USDTRY_2026-11-24` → compacted topic'te **günlük son kur** tutulur.
```bash
python p06_rest_api_pandas_kafka.py --dosya ../data/ornek_kur.json     # ağsız deneme
python p06_rest_api_pandas_kafka.py --url "https://api.frankfurter.app/latest?from=USD&to=TRY,EUR"
```

## 5.12 Batch / micro-batch uygulamaları — tasarım kontrol listesi

- [ ] Batch tetikleyicisi: boyut **ve** zaman
- [ ] Batch işleme süresi < `max.poll.interval.ms` (pay bırakın)
- [ ] Offset commit **hedef yazımından sonra** (veya aynı transaction'da)
- [ ] Hedef yazım idempotent (upsert / unique key)
- [ ] Reddedilenler DLT'ye, metrikleri loglanıyor
- [ ] Bellek: `batch_boyutu × ortalama_mesaj × ~5–10` (DataFrame ek yükü) RAM'e sığıyor
- [ ] Partition sayısı kadar süreçle yatay ölçeklenebilir (her süreç kendi micro-batch'i)

---

## 5.13 Pandas ne zaman uygun, ne zaman Spark/Flink?

| Durum | Pandas consumer | Spark Structured Streaming | Flink |
|---|---|---|---|
| Hacim | ~ < 5–10 bin olay/sn **toplam**, batch başı < birkaç yüz MB | Yüksek, büyük micro-batch'ler | Çok yüksek, düşük gecikme |
| Gecikme hedefi | Saniyeler–dakikalar | Saniyeler (micro-batch); düşük gecikme modu gelişiyor | Milisaniyeler |
| Batch'ler arası state | Yok (dış depo gerekir) | Var (state store, watermark) | **Güçlü** (keyed state, RocksDB, checkpoint) |
| Event-time pencere + geç veri | Batch içinde, elle | Watermark | Watermark, allowed lateness, side output |
| Join (stream–stream) | Pratik değil | Var | Var (interval, temporal) |
| Hata toleransı | Sizin kodunuz | Checkpoint | Checkpoint + savepoint |
| Ekip yetkinliği | Python/veri bilimi | PySpark | Java/SQL (PyFlink var) |
| Operasyon | Bir Python servisi | Spark cluster / Databricks | Flink cluster / K8s operator |

> Rakamlar kesin eşik değil, **başlangıç noktasıdır**: mesaj boyutu, işlem karmaşıklığı ve donanım belirleyicidir. Ölçüp karar verin.

**Spark/Flink'e geçiş sinyalleri:**
1. Lag sürekli artıyor ve partition başına bir Python süreci yetmiyor.
2. "Son N dakika/saat" gibi **batch'ler arası state** gereken özellikler (fraud, limit) çoğaldı.
3. İki akışı zaman penceresiyle **join** etmeniz gerekiyor.
4. Geç gelen veriyi doğru pencereye yerleştirme ve sonucu düzeltme ihtiyacı.
5. Exactly-once state + çıktı garantisi gerekiyor.

Ara çözüm: **Polars** (çok çekirdekli, Arrow tabanlı, Pandas'tan çok daha hızlı) veya **DuckDB** ile micro-batch işleme — dağıtık değildir ama tek makinede büyük kazanç sağlar.

## Kaynaklar

- [pandas 3.0 — What's new](https://pandas.pydata.org/docs/whatsnew/index.html)
- [pandas — Copy-on-Write](https://pandas.pydata.org/docs/user_guide/copy_on_write.html)
- [pandas — pd.Grouper / resample](https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html)
- [pandas.read_sql (coerce_float)](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html)
- [psycopg 3 — Transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)
- [PostgreSQL — INSERT ... ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [Spark Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
- [Apache Flink — Kafka connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/)
