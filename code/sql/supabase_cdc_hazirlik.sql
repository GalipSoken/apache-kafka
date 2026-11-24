-- =============================================================================
-- Supabase'i Debezium CDC KAYNAĞI olarak hazırlama
-- Dosya: code/sql/supabase_cdc_hazirlik.sql
-- Supabase SQL Editor'da (postgres kullanıcısıyla) çalıştırın.
-- Supabase'de wal_level zaten 'logical'dır; postgresql.conf / pg_hba.conf düzenlenemez.
-- Örnek kaynak şema: bddk (kendi şema/tablolarınızla değiştirin)
-- =============================================================================

-- 0) Kontrol
SHOW wal_level;                                   -- 'logical' olmalı
SELECT current_setting('max_replication_slots'), current_setting('max_wal_senders');

-- 1) CDC'ye özel, en az yetkili rol (postgres admin kullanıcısını KULLANMAYIN)
CREATE ROLE debezium_cdc WITH LOGIN REPLICATION PASSWORD 'GUCLU_BIR_SIFRE';
GRANT USAGE ON SCHEMA bddk TO debezium_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA bddk TO debezium_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA bddk GRANT SELECT ON TABLES TO debezium_cdc;

-- 2) UPDATE/DELETE'te tam "before" görüntüsü (WAL hacmini artırır; gerekiyorsa açın)
-- ALTER TABLE bddk.<tablo> REPLICA IDENTITY FULL;

-- 3) Publication (şemadaki tüm tablolar; PG15+ söz dizimi)
CREATE PUBLICATION dbz_supabase_pub FOR TABLES IN SCHEMA bddk;

-- 4) Heartbeat tablosu: az değişen veritabanında slot'un ilerlemesini (WAL'ın temizlenmesini) sağlar
CREATE SCHEMA IF NOT EXISTS cdc_ops;
CREATE TABLE IF NOT EXISTS cdc_ops.debezium_heartbeat (id INT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL);
GRANT USAGE ON SCHEMA cdc_ops TO debezium_cdc;
GRANT INSERT, UPDATE, SELECT ON cdc_ops.debezium_heartbeat TO debezium_cdc;
ALTER PUBLICATION dbz_supabase_pub ADD TABLE cdc_ops.debezium_heartbeat;

-- 5) İzleme: slot'un tuttuğu WAL miktarı (Supabase disk kotasını doldurabilir!)
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS gecikme
FROM pg_replication_slots;

-- 6) Pipeline'ı kalıcı olarak kapatırken slot'u MUTLAKA silin:
-- SELECT pg_drop_replication_slot('debezium_supabase_bddk');
