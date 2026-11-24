# 9. SQL ile Streaming Sorguları

> **Bu bölümde:** Normal (batch) SQL ile streaming SQL farkı; SELECT, WHERE, GROUP BY, JOIN, COUNT, SUM, AVG, TUMBLING, HOPPING, SESSION WINDOW ve event-time aggregation'ın **üç ayrı söz diziminde** gösterimi.
>
> ⚠️ **Söz dizimi kuralı:** Her kod bloğunun başlığı hangi motora ait olduğunu söyler: **PostgreSQL 16**, **ksqlDB (Confluent Platform 8.x)**, **Flink SQL 2.3**. Bu üçü birbirinin yerine **kullanılamaz.**
>
> **Kod:** [`code/sql/postgres_batch_karsiliklari.sql`](../code/sql/postgres_batch_karsiliklari.sql) (PostgreSQL 16.15'te çalıştırıldı ✅), [`code/ksqldb/01_islemler.sql`](../code/ksqldb/01_islemler.sql), [`code/flink/01_islemler.sql`](../code/flink/01_islemler.sql) (bu ortamda çalıştırılamadı; resmi dokümana göre yazıldı).

---

## 9.1 Normal SQL ile Streaming SQL arasındaki farklar

| Boyut | Normal SQL (PostgreSQL) | Streaming SQL (ksqlDB, Flink SQL) |
|---|---|---|
| Veri | Sınırlı tablo | Sınırsız akış (+ akıştan türeyen tablolar) |
| Sorgu ömrü | Çalışır, sonuç döner, **biter** | **Sürekli çalışır** (persistent / continuous query) |
| Sonuç | Tek seferlik küme | Sürekli güncellenen akış veya tablo (changelog) |
| Zaman | Kolon gibi davranır | **Birinci sınıf kavram**: event-time, watermark, pencere |
| GROUP BY | Tüm veride bir kez | Pencere içinde veya sonsuz güncellenen sonuç |
| JOIN | Tüm tablolar hazır | Zaman sınırlı (interval/window) veya tablo durumu ile |
| ORDER BY / LIMIT | Serbest | Sınırlı (sonsuz akışı sıralamak imkânsız; sadece zaman sırası/top-N) |
| Geç veri | Kavram yok | Grace period / allowed lateness |
| Çıktı modu | — | Append (sadece ekleme) vs. update/retract (önceki sonucu düzelt) |
| Kalıcılık | `CREATE VIEW` sorgu tanımıdır | `CREATE TABLE ... AS SELECT` (ksqlDB) / `INSERT INTO ... SELECT` (Flink) **çalışan bir iş** başlatır |

**Push vs. pull query (ksqlDB):**
- `SELECT ... EMIT CHANGES` → **push**: sonuçlar akmaya devam eder.
- `SELECT ... FROM materialized_table WHERE key = ...` (EMIT CHANGES yok) → **pull**: anlık durum, döner ve biter.

**Hazırlık:** Örnekler `islemler.json` topic'indeki olayları kullanır (`python code/python/03_json_producer.py --adet 5000`). Olay şeması: `hareket_id, musteri_id, islem_tipi, tutar (string), para_birimi, islem_zamani, islem_ts_ms (epoch ms), kanal`.

---

## 9.2 Kaynak tanımı

**PostgreSQL 16**
```sql
-- Tablo zaten var: public.hesap_hareket (code/sql/source_init.sql)
```

**ksqlDB**
```sql
CREATE STREAM islem_akisi (
    musteri_key VARCHAR KEY, hareket_id BIGINT, musteri_id BIGINT, islem_tipi VARCHAR,
    tutar VARCHAR, para_birimi VARCHAR, islem_ts_ms BIGINT, kanal VARCHAR
) WITH (KAFKA_TOPIC='islemler.json', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', TIMESTAMP='islem_ts_ms');
```

**Flink SQL 2.3**
```sql
CREATE TABLE islem_akisi (
    hareket_id BIGINT, musteri_id BIGINT, islem_tipi STRING, tutar DECIMAL(18, 2),
    para_birimi STRING, islem_ts_ms BIGINT, kanal STRING,
    olay_zamani AS TO_TIMESTAMP_LTZ(islem_ts_ms, 3),
    WATERMARK FOR olay_zamani AS olay_zamani - INTERVAL '5' SECOND
) WITH ('connector'='kafka', 'topic'='islemler.json', 'properties.bootstrap.servers'='kafka:19092',
        'properties.group.id'='flink-rehber', 'scan.startup.mode'='earliest-offset', 'format'='json');
```

---

## 9.3 SELECT ve WHERE

**PostgreSQL 16**
```sql
SELECT hareket_id, islem_tipi, tutar FROM public.hesap_hareket WHERE islem_tipi = 'EFT';
```

**ksqlDB** (push query — `EMIT CHANGES` zorunlu)
```sql
SELECT hareket_id, islem_tipi, CAST(tutar AS DECIMAL(18,2)) AS tutar
FROM islem_akisi WHERE islem_tipi = 'EFT' EMIT CHANGES LIMIT 10;
```
Kalıcı hali (yeni topic'e sürekli yazar):
```sql
CREATE STREAM buyuk_islemler WITH (KAFKA_TOPIC='islemler.buyuk', VALUE_FORMAT='JSON', PARTITIONS=6) AS
SELECT musteri_key, hareket_id, musteri_id, islem_tipi, CAST(tutar AS DECIMAL(18,2)) AS tutar, kanal
FROM islem_akisi WHERE CAST(tutar AS DECIMAL(18,2)) >= 100000 EMIT CHANGES;
```

**Flink SQL 2.3**
```sql
SELECT hareket_id, islem_tipi, tutar FROM islem_akisi WHERE islem_tipi = 'EFT';
```

## 9.4 GROUP BY + COUNT, SUM, AVG (pencere olmadan)

**PostgreSQL 16**
```sql
SELECT islem_tipi, count(*) AS adet, sum(tutar) AS toplam, round(avg(tutar), 2) AS ortalama
FROM public.hesap_hareket GROUP BY islem_tipi;
```

**ksqlDB** — sonuç bir **TABLE**'dır; her yeni olayda ilgili satır güncellenir (sonsuz büyüyen sayaç)
```sql
CREATE TABLE tip_bazinda_toplam AS
SELECT islem_tipi, COUNT(*) AS adet, SUM(CAST(tutar AS DECIMAL(18,2))) AS toplam,
       AVG(CAST(tutar AS DOUBLE)) AS ortalama_yaklasik
FROM islem_akisi GROUP BY islem_tipi EMIT CHANGES;
```
> ksqlDB'de `AVG` `DOUBLE` döndürür; finansal raporlamada `SUM / COUNT`'u kendiniz `DECIMAL` olarak hesaplayın.

**Flink SQL 2.3** — penceresiz `GROUP BY` **update/retract** akışı üretir (her olay önceki sonucu geri alıp yenisini yayar); sink'in upsert desteklemesi gerekir (`upsert-kafka`, JDBC)
```sql
SELECT islem_tipi, COUNT(*) AS adet, SUM(tutar) AS toplam, AVG(tutar) AS ortalama
FROM islem_akisi GROUP BY islem_tipi;
```

## 9.5 TUMBLING WINDOW (1 dakika, event-time)

**PostgreSQL 16** (✅ test edildi)
```sql
SELECT date_bin('1 minute', islem_zamani, TIMESTAMPTZ '2000-01-01') AS pencere_baslangic,
       islem_tipi, count(*) AS islem_adedi, sum(tutar) AS toplam_tutar, round(avg(tutar), 2) AS ortalama
FROM public.hesap_hareket GROUP BY 1, 2 ORDER BY 1, 2;
```

**ksqlDB**
```sql
CREATE TABLE islem_ozet_1dk WITH (KAFKA_TOPIC='islem_ozet_1dk', VALUE_FORMAT='JSON') AS
SELECT islem_tipi, WINDOWSTART AS pencere_baslangic, WINDOWEND AS pencere_bitis,
       COUNT(*) AS islem_adedi, SUM(CAST(tutar AS DECIMAL(18,2))) AS toplam_tutar
FROM islem_akisi
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 2 MINUTES)
GROUP BY islem_tipi EMIT CHANGES;
```
Pencere kapanana kadar ara sonuçları değil sadece kesin sonucu görmek için: `EMIT FINAL` (ksqlDB'de tumbling/hopping pencerelerde desteklenir).

**Flink SQL 2.3** (Window TVF)
```sql
SELECT window_start, window_end, islem_tipi,
       COUNT(*) AS islem_adedi, SUM(tutar) AS toplam_tutar, AVG(tutar) AS ortalama_tutar
FROM TABLE(TUMBLE(TABLE islem_akisi, DESCRIPTOR(olay_zamani), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, islem_tipi;
```
Flink window TVF aggregation'ı pencere watermark ile kapanınca **tek, kesin (append) sonuç** yayar.

## 9.6 HOPPING WINDOW (5 dk boyut, 1 dk adım)

**PostgreSQL 16** (✅ test edildi — her olay kapsadığı tüm pencerelere sayılır)
```sql
WITH pencereler AS (
  SELECT generate_series(date_trunc('minute', min(islem_zamani)) - interval '4 minutes',
                         date_trunc('minute', max(islem_zamani)), interval '1 minute') AS bas
  FROM public.hesap_hareket)
SELECT p.bas, p.bas + interval '5 minutes' AS bitis, h.musteri_id, count(*), sum(h.tutar)
FROM pencereler p JOIN public.hesap_hareket h
  ON h.islem_zamani >= p.bas AND h.islem_zamani < p.bas + interval '5 minutes'
GROUP BY 1, 2, 3 ORDER BY 1, 3;
```

**ksqlDB**
```sql
CREATE TABLE musteri_5dk_kayan AS
SELECT musteri_id, COUNT(*) AS islem_adedi, SUM(CAST(tutar AS DECIMAL(18,2))) AS toplam_tutar
FROM islem_akisi
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE, GRACE PERIOD 1 MINUTE)
GROUP BY musteri_id EMIT CHANGES;
```

**Flink SQL 2.3** — `HOP(tablo, zaman, KAYMA_ADIMI, PENCERE_BOYU)`: **önce adım, sonra boyut**
```sql
SELECT window_start, window_end, musteri_id, COUNT(*) AS adet, SUM(tutar) AS toplam
FROM TABLE(HOP(TABLE islem_akisi, DESCRIPTOR(olay_zamani), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY window_start, window_end, musteri_id;
```

## 9.7 SESSION WINDOW (30 sn hareketsizlik)

**PostgreSQL 16** (✅ test edildi — "gaps and islands")
```sql
WITH s AS (
  SELECT musteri_id, islem_zamani,
         CASE WHEN islem_zamani - lag(islem_zamani) OVER w > interval '30 seconds'
                OR lag(islem_zamani) OVER w IS NULL THEN 1 ELSE 0 END AS yeni_oturum
  FROM public.hesap_hareket WINDOW w AS (PARTITION BY musteri_id ORDER BY islem_zamani)
), o AS (SELECT *, sum(yeni_oturum) OVER (PARTITION BY musteri_id ORDER BY islem_zamani) AS oturum_no FROM s)
SELECT musteri_id, oturum_no, min(islem_zamani), max(islem_zamani), count(*)
FROM o GROUP BY 1, 2 ORDER BY 1, 2;
```

**ksqlDB**
```sql
CREATE TABLE musteri_oturum AS
SELECT musteri_id, WINDOWSTART AS oturum_baslangic, WINDOWEND AS oturum_bitis, COUNT(*) AS islem_adedi
FROM islem_akisi WINDOW SESSION (30 SECONDS) GROUP BY musteri_id EMIT CHANGES;
```

**Flink SQL 2.3**
```sql
SELECT window_start, window_end, musteri_id, COUNT(*) AS adet
FROM TABLE(SESSION(TABLE islem_akisi PARTITION BY musteri_id, DESCRIPTOR(olay_zamani), INTERVAL '30' SECOND))
GROUP BY window_start, window_end, musteri_id;
```

## 9.8 JOIN

### Stream–Table (olayı güncel referans veriyle zenginleştirme)

**PostgreSQL 16**
```sql
SELECT h.hareket_id, h.islem_tipi, h.tutar, m.segment, m.sehir
FROM public.hesap_hareket h LEFT JOIN public.musteri m USING (musteri_id);
```

**ksqlDB**
```sql
CREATE TABLE musteri_profil (musteri_key VARCHAR PRIMARY KEY, segment VARCHAR, sehir VARCHAR)
  WITH (KAFKA_TOPIC='musteri.profil', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');

CREATE STREAM islem_zengin AS
SELECT i.musteri_key, i.hareket_id, i.islem_tipi, CAST(i.tutar AS DECIMAL(18,2)) AS tutar, p.segment, p.sehir
FROM islem_akisi i LEFT JOIN musteri_profil p ON i.musteri_key = p.musteri_key EMIT CHANGES;
```
Koşul: Stream–table join'de iki tarafın **key'i ve partition sayısı** uyumlu olmalı (co-partitioning); değilse ksqlDB'de `PARTITION BY` ile yeniden anahtarlayın.

**Flink SQL 2.3** — temporal join (olay **anındaki** profil sürümü):
```sql
-- musteri_profil: upsert-kafka tablosu, PRIMARY KEY + event-time watermark tanımlı olmalı
SELECT i.hareket_id, i.tutar, p.segment
FROM islem_akisi AS i
LEFT JOIN musteri_profil FOR SYSTEM_TIME AS OF i.olay_zamani AS p
  ON i.musteri_id = p.musteri_id;
```

### Stream–Stream (zaman sınırlı)

**ksqlDB**
```sql
SELECT e.musteri_id, CAST(e.tutar AS DECIMAL(18,2)) AS eft, CAST(a.tutar AS DECIMAL(18,2)) AS atm
FROM islem_akisi e
JOIN islem_akisi a WITHIN 10 MINUTES ON e.musteri_key = a.musteri_key
WHERE e.islem_tipi = 'EFT' AND a.islem_tipi = 'ATM' EMIT CHANGES;
```
> ksqlDB'de self-join kısıtları olabilir; pratikte EFT ve ATM için iki ayrı stream (`CREATE STREAM eft AS SELECT ... WHERE islem_tipi='EFT'`) tanımlayıp onları join etmek daha nettir.

**Flink SQL 2.3** — interval join
```sql
SELECT e.musteri_id, e.tutar AS eft_tutar, a.tutar AS atm_tutar
FROM islem_akisi e, islem_akisi a
WHERE e.musteri_id = a.musteri_id AND e.islem_tipi = 'EFT' AND a.islem_tipi = 'ATM'
  AND a.olay_zamani BETWEEN e.olay_zamani AND e.olay_zamani + INTERVAL '10' MINUTE;
```

## 9.9 Event-time aggregation — neden önemli?

Aynı veri, **processing-time** ile hesaplanırsa:
- Gecikmeli gelen olaylar (mobil cihaz çevrimdışıydı, CDC 2 dk geride) **yanlış pencereye** düşer.
- Sistemi yeniden oynatırsanız (replay) sonuçlar **değişir** (çünkü işleme zamanı değişti).

Event-time ile:
- Pencere = olayın gerçekleştiği dakika → **tekrarlanabilir, denetlenebilir** sonuç (finansal raporlamada şart).
- Geç veri watermark/grace ile kontrollü ele alınır.

Motorlarda event-time'ı belirleme:
| Motor | Nasıl |
|---|---|
| PostgreSQL | Zaten kolon; `GROUP BY date_bin(..., islem_zamani, ...)` |
| ksqlDB | `WITH (TIMESTAMP='islem_ts_ms')` veya `TIMESTAMP_FORMAT` ile string kolon; belirtilmezse Kafka record timestamp (`ROWTIME`) |
| Flink SQL | Hesaplanan kolon + `WATERMARK FOR olay_zamani AS ...`; veya Kafka metadata: `ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` |
| Kafka Streams | Record timestamp veya özel `TimestampExtractor` |
| Spark | `withWatermark("olay_zamani", "2 minutes")` |

Bu rehberin producer'ı (`03_json_producer.py`) hem `islem_ts_ms` alanını yazar hem de Kafka record timestamp'ini aynı değere ayarlar; böylece her motor tutarlı event-time görür.

## 9.10 Sık hatalar

| Belirti | Motor | Sebep / çözüm |
|---|---|---|
| Sorgu hiç sonuç döndürmüyor | ksqlDB | `auto.offset.reset=latest` (varsayılan) → `SET 'auto.offset.reset'='earliest';` |
| Pencere sonucu hiç gelmiyor | Flink | Watermark ilerlemiyor: bazı partition'lar boş/idle → `'scan.watermark.idle-timeout'` veya `table.exec.source.idle-timeout` |
| `DECIMAL` alanı null | ksqlDB/Flink | JSON'da string tutar → `VARCHAR` + `CAST` |
| Join sonuç vermiyor | ksqlDB | Co-partitioning yok (key formatı veya partition sayısı farklı) |
| Sink "doesn't support consuming update changes" | Flink | Penceresiz GROUP BY update akışı üretir → `upsert-kafka` veya pencereli sorgu |
| Session window PostgreSQL'de tekrar sayıyor | PostgreSQL | `lag()` partition/order eksik |

## Kaynaklar

- [ksqlDB — Syntax reference](https://docs.confluent.io/platform/current/ksqldb/developer-guide/syntax-reference.html)
- [ksqlDB — Time and Windows](https://docs.confluent.io/platform/current/ksqldb/concepts/time-and-windows-in-ksqldb-queries.html)
- [ksqlDB — Joins](https://docs.confluent.io/platform/current/ksqldb/developer-guide/joins/overview.html)
- [Flink SQL — Window TVF](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/)
- [Flink SQL — Joins (interval, temporal)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/)
- [Flink — Kafka / Upsert Kafka SQL connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/)
- [PostgreSQL — date_bin](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-BIN)
- [PostgreSQL — Window functions](https://www.postgresql.org/docs/current/tutorial-window.html)
