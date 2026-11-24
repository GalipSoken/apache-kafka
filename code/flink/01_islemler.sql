-- =============================================================================
-- Flink SQL streaming örnekleri  (söz dizimi: Apache Flink 2.3 SQL)
-- Dosya: code/flink/01_islemler.sql
-- Çalıştırma: docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/rehber/01_islemler.sql
-- DİKKAT: Bu söz dizimi ksqlDB veya PostgreSQL DEĞİLDİR.
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

-- 1) Kaynak tablo: Kafka topic + event-time + WATERMARK (5 sn gecikme toleransı)
CREATE TABLE islem_akisi (
    hareket_id   BIGINT,
    musteri_id   BIGINT,
    islem_tipi   STRING,
    tutar        DECIMAL(18, 2),     -- JSON string "1250.75" Flink json formatında DECIMAL'e çözülür*
    para_birimi  STRING,
    islem_ts_ms  BIGINT,
    kanal        STRING,
    olay_zamani  AS TO_TIMESTAMP_LTZ(islem_ts_ms, 3),
    WATERMARK FOR olay_zamani AS olay_zamani - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'islemler.json',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'flink-rehber',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
-- * Kendi ortamınızda doğrulayın; çözülmezse tutar'ı STRING tanımlayıp CAST(tutar AS DECIMAL(18,2)) kullanın.

-- 2) SELECT / WHERE
-- SELECT hareket_id, islem_tipi, tutar FROM islem_akisi WHERE islem_tipi = 'EFT';

-- 3) TUMBLING WINDOW (Window TVF) + GROUP BY + COUNT/SUM/AVG
-- SELECT window_start, window_end, islem_tipi,
--        COUNT(*) AS islem_adedi, SUM(tutar) AS toplam_tutar, AVG(tutar) AS ortalama_tutar
-- FROM TABLE(TUMBLE(TABLE islem_akisi, DESCRIPTOR(olay_zamani), INTERVAL '1' MINUTE))
-- GROUP BY window_start, window_end, islem_tipi;

-- 4) HOPPING WINDOW (Flink'te HOP: önce kayma adımı, sonra pencere boyu)
-- SELECT window_start, window_end, musteri_id, COUNT(*) AS adet, SUM(tutar) AS toplam
-- FROM TABLE(HOP(TABLE islem_akisi, DESCRIPTOR(olay_zamani), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
-- GROUP BY window_start, window_end, musteri_id;

-- 5) SESSION WINDOW (müşteri bazında 30 sn boşluk)
-- SELECT window_start, window_end, musteri_id, COUNT(*) AS adet
-- FROM TABLE(SESSION(TABLE islem_akisi PARTITION BY musteri_id, DESCRIPTOR(olay_zamani), INTERVAL '30' SECOND))
-- GROUP BY window_start, window_end, musteri_id;

-- 6) Sonucu Kafka'ya yazan sink (upsert-kafka: key bazında son değer)
CREATE TABLE islem_ozet_1dk_sink (
    pencere_baslangic TIMESTAMP(3),
    islem_tipi        STRING,
    islem_adedi       BIGINT,
    toplam_tutar      DECIMAL(20, 2),
    PRIMARY KEY (pencere_baslangic, islem_tipi) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'flink.islem_ozet_1dk',
    'properties.bootstrap.servers' = 'kafka:19092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO islem_ozet_1dk_sink
SELECT window_start, islem_tipi, COUNT(*), SUM(tutar)
FROM TABLE(TUMBLE(TABLE islem_akisi, DESCRIPTOR(olay_zamani), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, islem_tipi;

-- 7) Interval JOIN (iki stream): aynı müşterinin 10 dk içinde EFT ve ardından ATM çekimi
-- SELECT e.musteri_id, e.tutar AS eft_tutar, a.tutar AS atm_tutar, e.olay_zamani, a.olay_zamani
-- FROM islem_akisi e, islem_akisi a
-- WHERE e.musteri_id = a.musteri_id
--   AND e.islem_tipi = 'EFT' AND a.islem_tipi = 'ATM'
--   AND a.olay_zamani BETWEEN e.olay_zamani AND e.olay_zamani + INTERVAL '10' MINUTE;
