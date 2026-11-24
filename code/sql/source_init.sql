-- =============================================================================
-- Kaynak PostgreSQL (bankdb) başlangıç script'i
-- Dosya: code/sql/source_init.sql
-- docker-compose.cdc.yml içinde /docker-entrypoint-initdb.d/ altına mount edilir;
-- container İLK kez (boş volume ile) başladığında bir kez çalışır.
-- PostgreSQL 14-18 ile uyumludur (test: PostgreSQL 16 üzerinde çalıştırıldı).
-- =============================================================================

-- 1) İş tabloları ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.musteri (
    musteri_id   BIGINT PRIMARY KEY,
    ad_soyad     TEXT        NOT NULL,
    segment      TEXT        NOT NULL CHECK (segment IN ('BIREYSEL','KOBI','KURUMSAL')),
    sehir        TEXT,
    guncellendi  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.hesap_hareket (
    hareket_id   BIGINT PRIMARY KEY,
    musteri_id   BIGINT        NOT NULL REFERENCES public.musteri(musteri_id),
    islem_tipi   TEXT          NOT NULL CHECK (islem_tipi IN ('EFT','HAVALE','KART','ATM','FAST')),
    tutar        NUMERIC(18,2) NOT NULL,          -- finansal tutar: asla FLOAT değil
    para_birimi  CHAR(3)       NOT NULL DEFAULT 'TRY',
    islem_zamani TIMESTAMPTZ   NOT NULL,
    aciklama     TEXT
);

-- 2) UPDATE/DELETE olaylarında "before" alanının TAM dolu gelmesi için -----------
--    Varsayılan (DEFAULT) replica identity'de DELETE'te sadece PK gelir,
--    UPDATE'te before = null olur.
ALTER TABLE public.musteri       REPLICA IDENTITY FULL;
ALTER TABLE public.hesap_hareket REPLICA IDENTITY FULL;

-- 3) Debezium için en az yetkili kullanıcı -----------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium_dev_pw';
  END IF;
END $$;

GRANT CONNECT ON DATABASE bankdb TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.musteri, public.hesap_hareket TO debezium;   -- initial snapshot için

-- 4) Publication: hangi tabloların değişikliği yayınlanacak? ------------------------
--    Debezium'a publication.autocreate.mode=disabled verdiğimiz için biz açıyoruz;
--    böylece Debezium kullanıcısına tablo sahipliği vermek gerekmez.
DROP PUBLICATION IF EXISTS dbz_bank_pub;
CREATE PUBLICATION dbz_bank_pub FOR TABLE public.musteri, public.hesap_hareket;

-- 5) Örnek veri -----------------------------------------------------------------------
INSERT INTO public.musteri (musteri_id, ad_soyad, segment, sehir) VALUES
  (1, 'Ayşe Yılmaz',   'BIREYSEL', 'İstanbul'),
  (2, 'Demir Lojistik','KOBI',     'Kocaeli'),
  (3, 'Atlas Enerji',  'KURUMSAL', 'Ankara')
ON CONFLICT DO NOTHING;

INSERT INTO public.hesap_hareket (hareket_id, musteri_id, islem_tipi, tutar, islem_zamani, aciklama) VALUES
  (1001, 1, 'KART',   1250.75,   '2026-11-24 09:15:00+03', 'Market'),
  (1002, 2, 'EFT',    98000.00,  '2026-11-24 09:20:00+03', 'Tedarikçi ödemesi'),
  (1003, 3, 'HAVALE', 1500000.10,'2026-11-24 09:25:00+03', 'Grup içi transfer')
ON CONFLICT DO NOTHING;
