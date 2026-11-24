# 16. Cloud Entegrasyonu: Supabase

> **Bu bölümde:** Rehberdeki zincirin son halkası — Supabase'in (yönetilen PostgreSQL) Kafka mimarisinde **kaynak (CDC ile)** ve **hedef (sink)** olarak kullanımı; bağlantı türleri, rol/yetki, publication/slot, WAL ve disk riski, güvenlik ve mimari seçenekler.
>
> **Kod:** [`code/sql/supabase_cdc_hazirlik.sql`](../code/sql/supabase_cdc_hazirlik.sql) (✅ yerel PostgreSQL 16'da `bddk` örnek şemasıyla çalıştırıldı; Supabase'de çalıştırılmadı), [`code/connect/supabase-source-debezium.json`](../code/connect/supabase-source-debezium.json), [`code/connect/supabase-sink-debezium-jdbc.json`](../code/connect/supabase-sink-debezium-jdbc.json), [`code/connect/secrets/supabase.properties.example`](../code/connect/secrets/supabase.properties.example)

---

## 16.1 Konumlandırma

Supabase bir **Kafka servisi değildir**; Postgres + Auth + Storage + Realtime + Edge Functions sunan bir platformdur. Kafka (managed veya kendi sunucunuz) ile ilişkisi:

```mermaid
flowchart LR
  subgraph SB1[Supabase projesi A — kaynak]
    T1[(bddk.* tabloları)]
    WAL[[WAL + slot<br/>debezium_supabase_bddk]]
    T1 --> WAL
  end
  subgraph KAFKA[Kafka — managed veya self-hosted]
    DZ[Kafka Connect<br/>Debezium PostgresConnector]
    TOP[(supabase.bddk.* topic'leri)]
    SNK[Kafka Connect<br/>Debezium JDBC Sink]
    FL[Flink / Python consumer]
  end
  subgraph SB2[Supabase projesi B — hedef]
    AN[(analitik.* tabloları)]
  end
  OTHER[(DWH / ClickHouse / Iceberg)]
  WAL -- "direct connection :5432<br/>sslmode=require" --> DZ --> TOP
  TOP --> FL --> TOP
  TOP --> SNK -- "direct / session pooler" --> AN
  TOP --> OTHER
```

| Senaryo | Supabase rolü | Araç |
|---|---|---|
| Supabase'deki değişiklikleri başka sistemlere akıtmak (DWH, arama, bildirim) | **Kaynak** | Debezium CDC |
| Kafka'daki olayları uygulamanın Supabase veritabanına yazmak (dashboard, API) | **Hedef** | Debezium JDBC Sink veya Python consumer (upsert) |
| İki Supabase projesi arasında senkronizasyon | Kaynak + hedef | Kafka üzerinden veya doğrudan PostgreSQL logical replication (Kafka gereksiz olabilir!) |
| Gerçek zamanlı UI güncellemeleri | — | Supabase **Realtime** (Kafka gerekmez) |

> **Kafka gerçekten gerekli mi?** Tek kaynak → tek hedef PostgreSQL senkronizasyonu için Supabase'in desteklediği yerel logical replication (`CREATE SUBSCRIPTION`) daha basittir. Kafka; çoklu tüketici, replay, akış işleme veya farklı hedef türleri gerektiğinde anlamlıdır ([17. bölüm](17-ne-zaman-kullanmamali.md)).

## 16.2 Supabase'i CDC kaynağı yapmak

### Ön koşullar ve kısıtlar
- Supabase'de `wal_level=logical` **zaten açıktır**; `postgresql.conf` / `pg_hba.conf` düzenlenemez.
- Logical replication için **direct connection** (`db.<project-ref>.supabase.co:5432`) kullanılmalıdır. **Supavisor pooler bağlantıları (transaction veya session) logical replication'ı desteklemez.**
- Direct connection varsayılan olarak **IPv6**'dır. Kafka Connect ortamınızda IPv6 çıkışı yoksa Supabase'in ücretli **dedicated IPv4 add-on**'u gerekir.
- Supabase'in `postgres` admin kullanıcısını connector için **kullanmayın**; ayrı, en az yetkili bir REPLICATION rolü açın.
- Proje **upgrade**'lerinde Supabase, logical replication slot'larının kaldırılmasını ister → upgrade öncesi connector'ı durdurup slot'u silin, sonrası yeniden oluşturup (gerekirse) snapshot alın.

### Adım 1 — Hazırlık SQL'i (Supabase SQL Editor)
[`code/sql/supabase_cdc_hazirlik.sql`](../code/sql/supabase_cdc_hazirlik.sql):
```sql
SHOW wal_level;                                             -- logical
CREATE ROLE debezium_cdc WITH LOGIN REPLICATION PASSWORD 'GUCLU_BIR_SIFRE';
GRANT USAGE ON SCHEMA bddk TO debezium_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA bddk TO debezium_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA bddk GRANT SELECT ON TABLES TO debezium_cdc;
CREATE PUBLICATION dbz_supabase_pub FOR TABLES IN SCHEMA bddk;   -- PG15+

-- Az değişen DB'de slot'u ilerletmek için heartbeat tablosu
CREATE SCHEMA IF NOT EXISTS cdc_ops;
CREATE TABLE IF NOT EXISTS cdc_ops.debezium_heartbeat (id INT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL);
GRANT USAGE ON SCHEMA cdc_ops TO debezium_cdc;
GRANT INSERT, UPDATE, SELECT ON cdc_ops.debezium_heartbeat TO debezium_cdc;
ALTER PUBLICATION dbz_supabase_pub ADD TABLE cdc_ops.debezium_heartbeat;
```
> `bddk` örnek şemadır; kendi şemanızla değiştirin. Supabase'de `CREATE ROLE ... REPLICATION` yetkisinin projenizin `postgres` rolüyle verilebildiğini kendi projenizde doğrulayın; verilemiyorsa Supabase dokümantasyonundaki replikasyon rolü önerisini izleyin.

### Adım 2 — Connector
[`code/connect/supabase-source-debezium.json`](../code/connect/supabase-source-debezium.json):
```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "db.<PROJECT_REF>.supabase.co",
  "database.port": "5432",
  "database.user": "${file:/secrets/supabase.properties:cdc_user}",
  "database.password": "${file:/secrets/supabase.properties:cdc_password}",
  "database.dbname": "postgres",
  "database.sslmode": "require",
  "topic.prefix": "supabase",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_supabase_bddk",
  "publication.name": "dbz_supabase_pub",
  "publication.autocreate.mode": "disabled",
  "schema.include.list": "bddk",
  "decimal.handling.mode": "precise",
  "heartbeat.interval.ms": "60000",
  "heartbeat.action.query": "INSERT INTO cdc_ops.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
}
```
```bash
cp code/connect/secrets/supabase.properties.example code/connect/secrets/supabase.properties   # doldurun
python3 -c "import json;print(json.dumps(json.load(open('code/connect/supabase-source-debezium.json'))['config']))" \
 | curl -fs -X PUT -H 'Content-Type: application/json' --data @- localhost:8083/connectors/supabase-source-bddk/config
curl -s localhost:8083/connectors/supabase-source-bddk/status | python3 -m json.tool
```
Daha sıkı TLS için `database.sslmode=verify-full` + Supabase'in CA sertifikasını (`database.sslrootcert`) kullanın.

### Adım 3 — İzleme (Supabase SQL Editor)
```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS gecikme
FROM pg_replication_slots;
```
**Disk riski Supabase'de daha kritiktir:** Proje disk boyutu plan/ayarlarla sınırlıdır; connector günlerce kapalı kalırsa slot WAL biriktirir ve disk kotası dolabilir. Alarm kurun; pipeline'ı kalıcı kapatırken `SELECT pg_drop_replication_slot('debezium_supabase_bddk');` çalıştırın.

## 16.3 Supabase'i hedef (sink) yapmak

### Seçenek A — Debezium JDBC Sink
[`code/connect/supabase-sink-debezium-jdbc.json`](../code/connect/supabase-sink-debezium-jdbc.json):
```json
"connection.url": "jdbc:postgresql://db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require",
"insert.mode": "upsert", "primary.key.mode": "record_key", "delete.enabled": "true",
"schema.evolution": "none",
"collection.name.format": "analitik.hesap_hareket_replica"
```
Bağlantı türü (sink için):
| Bağlantı | Uygun mu? | Not |
|---|---|---|
| Direct connection `:5432` | ✅ | IPv6/IPv4 add-on konusu aynı |
| Supavisor **session** mode | ✅ | Kalıcı bağlantı; IPv4 sunar |
| Supavisor **transaction** mode `:6543` | ⚠️ | Sunucu tarafı prepared statement'lar desteklenmez → JDBC'de `prepareThreshold=0`; Python psycopg'de `prepare_threshold=None` |

Hedef tabloları önceden açın (finansal tipler: `NUMERIC(18,2)`, `TIMESTAMPTZ`), sink kullanıcısına sadece o şemada `INSERT/UPDATE/DELETE` verin. Supabase'de tablolar **PostgREST** ile API'ye açık olabilir → **Row Level Security (RLS)** politikalarını hedef tablolarda mutlaka etkinleştirin.

### Seçenek B — Python consumer (Pandas/psycopg)
Bu rehberin `11_batch_consume.py` ve `p04_kafka_pandas_postgres.py` örnekleri Supabase'e doğrudan bağlanabilir:
```bash
export PG_DSN="postgresql://sink_user:***@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require"
python code/pandas/p04_kafka_pandas_postgres.py
```
`target_init.sql`'deki tabloları (`islem_ham`, `islem_ozet_dakika`, `kafka_offsetleri`) önce Supabase'de oluşturun. Offset'leri Supabase'de veriyle aynı transaction'da saklayan p04 deseni burada da uçtan uca exactly-once sağlar.

## 16.4 Kafka'yı nerede çalıştırmalı? (Supabase ile)
| Seçenek | Artı | Eksi |
|---|---|---|
| Confluent Cloud / Aiven / MSK (Supabase ile aynı bölge) | Yönetilen; Debezium connector'ı managed olarak da çalıştırılabilir | Maliyet; veri bölge/regülasyon değerlendirmesi |
| Kendi sunucunuz / VM'de Docker (bu rehberin compose'u) | Tam kontrol, düşük maliyet | İşletim yükü; Supabase'e IPv6/IPv4 erişimi ve TLS sizde |
| Kafka'sız: Supabase → Supabase logical replication | En basit | Tek hedef, akış işleme yok |
| Kafka'sız: Supabase Database Webhooks / Edge Functions → HTTP | Hafif olay bildirimi | Garanti/replay yok, düşük hacim |

Bölge seçimi: Kafka Connect'i Supabase projesiyle **aynı bulut bölgesinde** çalıştırın (gecikme ve egress maliyeti).

## 16.5 Güvenlik özeti (Supabase)
- CDC ve sink için **ayrı roller**, en az yetki; `postgres` rolü sadece yönetim için.
- `sslmode=require` (tercihen `verify-full`).
- Secret'lar Connect config provider ile (connector JSON'unda düz şifre yok).
- Hedef tablolarda **RLS** (PostgREST üzerinden dışa açılma riski).
- Slot izleme alarmı + proje upgrade prosedürüne "slot'u kaldır" adımı.
- Kişisel veri içeren tablolar için KVKK değerlendirmesi (13.12) ve Supabase bölge/sözleşme koşulları.

## 16.6 Olası hatalar
| Hata | Sebep | Çözüm |
|---|---|---|
| `Network is unreachable` / bağlantı timeout | IPv6 çıkışı yok | IPv4 add-on veya IPv6 destekli ağ |
| Pooler adresiyle replikasyon bağlantısı kurulamıyor (hata metni pooler'a göre değişir) | Pooler adresi kullanıldı | Direct connection `db.<ref>.supabase.co:5432` |
| `must be superuser or replication role` | Rolde REPLICATION yok | Rolü yeniden yapılandırın (Supabase yetki modeli) |
| `prepared statement "S_1" already exists` (sink) | Transaction pooler | Direct/session, veya `prepareThreshold=0` |
| Disk doluluk uyarısı | Slot WAL biriktiriyor | Connector'ı çalıştırın veya slot'u silin; heartbeat |

## Kaynaklar

- [Supabase — Replication FAQ (hangi bağlantı dizesi?)](https://supabase.com/docs/guides/database/replication/faq)
- [Supabase — Replicate to another Postgres database using Logical Replication](https://supabase.com/docs/guides/database/postgres/setup-replication-external)
- [Supabase — Connecting to your database (direct, pooler, IPv4)](https://supabase.com/docs/guides/database/connecting-to-postgres)
- [Supabase — Dedicated IPv4 address](https://supabase.com/docs/guides/platform/ipv4-address)
- [Supabase — Upgrading (logical replication slot'ları)](https://supabase.com/docs/guides/platform/upgrading)
- [Supabase — Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security)
- [Redis — Prepare Supabase for RDI (Debezium tabanlı CDC için Supabase hazırlığı)](https://redis.io/docs/latest/integrate/redis-data-integration/data-pipelines/prepare-dbs/supabase/)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
