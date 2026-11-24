-- =============================================================================
-- Outbox Pattern: iş verisi ve olay AYNI veritabanı transaction'ında yazılır.
-- Dosya: code/sql/outbox_pattern.sql   (PostgreSQL 14+; test: PostgreSQL 16)
-- Debezium, outbox tablosunu CDC ile okuyup EventRouter SMT ile olayı Kafka'ya yönlendirir.
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.odeme (
    odeme_id     UUID PRIMARY KEY,
    musteri_id   BIGINT        NOT NULL,
    tutar        NUMERIC(18,2) NOT NULL,
    durum        TEXT          NOT NULL,
    olusturuldu  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.outbox (
    id             UUID        PRIMARY KEY,
    aggregatetype  TEXT        NOT NULL,      -- topic yönlendirmesi: outbox.event.<aggregatetype>
    aggregateid    TEXT        NOT NULL,      -- Kafka message key (sıralama!)
    type           TEXT        NOT NULL,      -- olay tipi (header olarak taşınır)
    payload        JSONB       NOT NULL,
    olusturuldu    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uygulamanın yaptığı tek transaction (dual-write YOK):
BEGIN;
INSERT INTO public.odeme (odeme_id, musteri_id, tutar, durum)
VALUES ('6f1c1d3e-6a9b-4c3e-9a51-6b7b0c1f2a10', 7, 98000.00, 'ALINDI');
INSERT INTO public.outbox (id, aggregatetype, aggregateid, type, payload)
VALUES ('0d2b5c4e-1f7a-4b2c-8e3d-9f0a1b2c3d4e', 'odeme', '7', 'OdemeAlindi',
        jsonb_build_object('odeme_id','6f1c1d3e-6a9b-4c3e-9a51-6b7b0c1f2a10','musteri_id',7,'tutar','98000.00'));
COMMIT;

-- Outbox tablosu büyümesin: Debezium olayı WAL'dan okuduğu için satır hemen silinebilir
-- (INSERT WAL'a yazıldı; DELETE olayını EventRouter yok sayar). Alternatif: periyodik temizlik.
DELETE FROM public.outbox WHERE olusturuldu < now() - interval '1 day';
