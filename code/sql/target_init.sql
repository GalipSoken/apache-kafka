-- =============================================================================
-- Hedef PostgreSQL (dwh) başlangıç script'i
-- Dosya: code/sql/target_init.sql
-- Debezium JDBC Sink tabloyu kendisi de oluşturabilir (schema.evolution=basic),
-- ancak finansal tiplerin (NUMERIC(18,2), TIMESTAMPTZ) kontrolü bizde kalsın diye
-- tabloyu önceden açıyoruz.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.hesap_hareket_replica (
    hareket_id   BIGINT PRIMARY KEY,
    musteri_id   BIGINT        NOT NULL,
    islem_tipi   TEXT          NOT NULL,
    tutar        NUMERIC(18,2) NOT NULL,
    para_birimi  CHAR(3)       NOT NULL,
    islem_zamani TIMESTAMPTZ   NOT NULL,
    aciklama     TEXT
);

CREATE TABLE IF NOT EXISTS public.musteri_replica (
    musteri_id   BIGINT PRIMARY KEY,
    ad_soyad     TEXT        NOT NULL,
    segment      TEXT        NOT NULL,
    sehir        TEXT,
    guncellendi  TIMESTAMPTZ NOT NULL
);

-- Pandas / Python consumer örneklerinin yazdığı analitik tablo
CREATE TABLE IF NOT EXISTS public.islem_ozet_dakika (
    pencere_baslangic TIMESTAMPTZ   NOT NULL,
    islem_tipi        TEXT          NOT NULL,
    islem_adedi       INTEGER       NOT NULL,
    toplam_tutar      NUMERIC(20,2) NOT NULL,
    ortalama_tutar    NUMERIC(20,2) NOT NULL,
    PRIMARY KEY (pencere_baslangic, islem_tipi)
);

-- Python consumer'ın idempotent upsert yaptığı tablo
CREATE TABLE IF NOT EXISTS public.islem_ham (
    hareket_id   BIGINT PRIMARY KEY,
    musteri_id   BIGINT        NOT NULL,
    islem_tipi   TEXT          NOT NULL,
    tutar        NUMERIC(18,2) NOT NULL,
    para_birimi  CHAR(3)       NOT NULL,
    islem_zamani TIMESTAMPTZ   NOT NULL,
    kafka_topic     TEXT    NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT  NOT NULL,
    yuklendi     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Offset'leri hedef DB'de, veriyle AYNI transaction içinde saklamak için
-- (Kafka -> PostgreSQL uçtan uca exactly-once deseni; bkz. code/pandas/p04_kafka_pandas_postgres.py)
CREATE TABLE IF NOT EXISTS public.kafka_offsetleri (
    consumer_group TEXT    NOT NULL,
    topic          TEXT    NOT NULL,
    partition_no   INTEGER NOT NULL,
    sonraki_offset BIGINT  NOT NULL,     -- bir sonraki okunacak offset (son işlenen + 1)
    guncellendi    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition_no)
);
