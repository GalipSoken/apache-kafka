-- =============================================================================
-- ksqlDB streaming SQL örnekleri  (söz dizimi: ksqlDB / Confluent Platform 8.x)
-- Dosya: code/ksqldb/01_islemler.sql
-- Çalıştırma: docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
--             ksql> RUN SCRIPT '/ksqldb/01_islemler.sql';
-- Veri: python code/python/03_json_producer.py --adet 5000   (topic: islemler.json)
-- DİKKAT: Bu söz dizimi PostgreSQL veya Flink SQL DEĞİLDİR.
-- =============================================================================

SET 'auto.offset.reset' = 'earliest';

-- 1) STREAM tanımı: topic üzerine şema. Event-time = islem_ts_ms (epoch ms)
--    tutar JSON'da string taşındığı için VARCHAR alıp DECIMAL'e CAST ediyoruz.
CREATE STREAM IF NOT EXISTS islem_akisi (
    musteri_key  VARCHAR KEY,
    hareket_id   BIGINT,
    musteri_id   BIGINT,
    islem_tipi   VARCHAR,
    tutar        VARCHAR,
    para_birimi  VARCHAR,
    islem_ts_ms  BIGINT,
    kanal        VARCHAR
) WITH (
    KAFKA_TOPIC  = 'islemler.json',
    KEY_FORMAT   = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP    = 'islem_ts_ms'
);

-- 2) SELECT + WHERE (push query: sonuçlar akmaya devam eder, EMIT CHANGES zorunlu)
--    CLI'da interaktif çalıştırın:
-- SELECT hareket_id, islem_tipi, CAST(tutar AS DECIMAL(18,2)) AS tutar
-- FROM islem_akisi WHERE islem_tipi = 'EFT' EMIT CHANGES LIMIT 10;

-- 3) Kalıcı filtre: büyük işlemleri yeni bir topic'e yaz (persistent query)
CREATE STREAM IF NOT EXISTS buyuk_islemler
  WITH (KAFKA_TOPIC = 'islemler.buyuk', VALUE_FORMAT = 'JSON', PARTITIONS = 6) AS
SELECT musteri_key, hareket_id, musteri_id, islem_tipi,
       CAST(tutar AS DECIMAL(18,2)) AS tutar, kanal
FROM islem_akisi
WHERE CAST(tutar AS DECIMAL(18,2)) >= 100000
EMIT CHANGES;

-- 4) GROUP BY + COUNT/SUM/AVG + TUMBLING WINDOW (1 dk, çakışmasız)
--    GRACE PERIOD: pencere kapandıktan sonra 2 dk'ya kadar geç gelen olay kabul edilir.
CREATE TABLE IF NOT EXISTS islem_ozet_1dk WITH (KAFKA_TOPIC = 'islem_ozet_1dk', VALUE_FORMAT = 'JSON') AS
SELECT islem_tipi,
       WINDOWSTART AS pencere_baslangic,
       WINDOWEND   AS pencere_bitis,
       COUNT(*)                                AS islem_adedi,
       SUM(CAST(tutar AS DECIMAL(18,2)))       AS toplam_tutar,
       AVG(CAST(tutar AS DOUBLE))              AS ortalama_tutar_yaklasik
FROM islem_akisi
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 2 MINUTES)
GROUP BY islem_tipi
EMIT CHANGES;
-- Not: ksqlDB'de AVG DOUBLE döndürür; finansal raporda SUM/COUNT ile kendiniz bölün.

-- 5) HOPPING WINDOW: 5 dk'lık pencere, her 1 dk'da bir kayar (pencereler çakışır)
CREATE TABLE IF NOT EXISTS musteri_5dk_kayan WITH (KAFKA_TOPIC = 'musteri_5dk_kayan', VALUE_FORMAT = 'JSON') AS
SELECT musteri_id,
       COUNT(*) AS islem_adedi,
       SUM(CAST(tutar AS DECIMAL(18,2))) AS toplam_tutar
FROM islem_akisi
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE, GRACE PERIOD 1 MINUTE)
GROUP BY musteri_id
EMIT CHANGES;

-- 6) SESSION WINDOW: aynı müşterinin 30 sn'den kısa aralıklarla yaptığı işlemler tek oturum
CREATE TABLE IF NOT EXISTS musteri_oturum WITH (KAFKA_TOPIC = 'musteri_oturum', VALUE_FORMAT = 'JSON') AS
SELECT musteri_id,
       WINDOWSTART AS oturum_baslangic,
       WINDOWEND   AS oturum_bitis,
       COUNT(*)    AS islem_adedi
FROM islem_akisi
WINDOW SESSION (30 SECONDS)
GROUP BY musteri_id
EMIT CHANGES;

-- 7) JOIN: stream-table (işlem akışını müşteri profiliyle zenginleştir)
--    musteri.profil compact topic'tir; key = musteri_id (string)
CREATE TABLE IF NOT EXISTS musteri_profil (
    musteri_key VARCHAR PRIMARY KEY,
    segment     VARCHAR,
    sehir       VARCHAR
) WITH (KAFKA_TOPIC = 'musteri.profil', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');

CREATE STREAM IF NOT EXISTS islem_zengin WITH (KAFKA_TOPIC = 'islemler.zengin', VALUE_FORMAT = 'JSON') AS
SELECT i.musteri_key, i.hareket_id, i.islem_tipi, CAST(i.tutar AS DECIMAL(18,2)) AS tutar,
       p.segment, p.sehir
FROM islem_akisi i
LEFT JOIN musteri_profil p ON i.musteri_key = p.musteri_key
EMIT CHANGES;

-- 8) Pull query (anlık durum sorgusu, materialized table üzerinde):
-- SELECT * FROM islem_ozet_1dk WHERE islem_tipi = 'EFT';
