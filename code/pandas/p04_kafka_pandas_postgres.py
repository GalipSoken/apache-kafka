"""
Kafka -> Pandas -> PostgreSQL, UÇTAN UCA EXACTLY-ONCE deseni.
Dosya: code/pandas/p04_kafka_pandas_postgres.py
Ön koşul: code/sql/target_init.sql (islem_ham, islem_ozet_dakika, kafka_offsetleri)
Çalıştırma:
  export PG_DSN="postgresql://postgres:postgres@localhost:5434/dwh"
  python p04_kafka_pandas_postgres.py

Nasıl: Offset'ler Kafka'ya DEĞİL, veriyle AYNI PostgreSQL transaction'ına yazılır.
 - on_assign: partition'ların başlangıç offset'i kafka_offsetleri tablosundan okunur (seek)
 - her batch: [ham upsert + özet yeniden hesap + offset upsert] tek transaction
Çökme hangi anda olursa olsun ya hepsi yazılır ya hiçbiri -> tekrar işleme etkisizdir.
"""
import os
import sys
from pathlib import Path

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from common import ShutdownFlag, consumer_config, setup_logging  # noqa: E402
from pandas_utils import messages_to_df, temizle  # noqa: E402

log = setup_logging("p04")
GROUP = "rehber-pandas-pg-eos"
TOPIC = "islemler.json"
DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5434/dwh")

# DİKKAT: itertuples() alt çizgiyle başlayan kolonları (_topic...) konumsal adlara (_6...) çevirir.
META_ADLARI = {"_topic": "k_topic", "_partition": "k_partition", "_offset": "k_offset"}

HAM_UPSERT = """
INSERT INTO public.islem_ham (hareket_id, musteri_id, islem_tipi, tutar, para_birimi, islem_zamani,
                              kafka_topic, kafka_partition, kafka_offset)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (hareket_id) DO UPDATE SET
  musteri_id=EXCLUDED.musteri_id, islem_tipi=EXCLUDED.islem_tipi, tutar=EXCLUDED.tutar,
  para_birimi=EXCLUDED.para_birimi, islem_zamani=EXCLUDED.islem_zamani,
  kafka_topic=EXCLUDED.kafka_topic, kafka_partition=EXCLUDED.kafka_partition,
  kafka_offset=EXCLUDED.kafka_offset, yuklendi=now()
"""
# Özet, ham tablodan etkilenen pencereler için YENİDEN hesaplanır -> idempotent
OZET_YENIDEN = """
INSERT INTO public.islem_ozet_dakika (pencere_baslangic, islem_tipi, islem_adedi, toplam_tutar, ortalama_tutar)
SELECT date_trunc('minute', islem_zamani), islem_tipi, count(*), sum(tutar), round(avg(tutar), 2)
FROM public.islem_ham
WHERE date_trunc('minute', islem_zamani) = ANY(%s)
GROUP BY 1, 2
ON CONFLICT (pencere_baslangic, islem_tipi) DO UPDATE SET
  islem_adedi=EXCLUDED.islem_adedi, toplam_tutar=EXCLUDED.toplam_tutar, ortalama_tutar=EXCLUDED.ortalama_tutar
"""
OFFSET_UPSERT = """
INSERT INTO public.kafka_offsetleri (consumer_group, topic, partition_no, sonraki_offset)
VALUES (%s,%s,%s,%s)
ON CONFLICT (consumer_group, topic, partition_no) DO UPDATE SET
  sonraki_offset=EXCLUDED.sonraki_offset, guncellendi=now()
"""


def main() -> None:
    conn = psycopg.connect(DSN)
    c = Consumer(consumer_config(GROUP, **{"enable.auto.commit": False}))

    def on_assign(consumer, partitions):
        with conn.cursor() as cur:
            cur.execute("SELECT partition_no, sonraki_offset FROM public.kafka_offsetleri "
                        "WHERE consumer_group=%s AND topic=%s", (GROUP, TOPIC))
            kayitli = dict(cur.fetchall())
        conn.commit()
        for tp in partitions:
            tp.offset = kayitli.get(tp.partition, OFFSET_BEGINNING)
        log.info("assign + DB offset'leri: %s", [(tp.partition, tp.offset) for tp in partitions])
        # Callback'te offset'leri değiştirmek yeterli: confluent-kafka, callback dönünce
        # protokole uygun olarak (classic: assign, consumer/cooperative: incremental_assign)
        # bu listeyi kendisi atar.

    c.subscribe([TOPIC], on_assign=on_assign)
    flag = ShutdownFlag().install(log)
    try:
        while flag.running:
            msgs = [m for m in c.consume(num_messages=1000, timeout=2.0) if m.error() is None]
            if not msgs:
                continue
            temiz, red = temizle(messages_to_df(msgs))
            son = {}
            for m in msgs:
                son[m.partition()] = max(son.get(m.partition(), -1), m.offset())
            with conn.transaction():
                with conn.cursor() as cur:
                    if not temiz.empty:
                        cur.executemany(HAM_UPSERT, [
                            (int(r.hareket_id), int(r.musteri_id), r.islem_tipi, r.tutar, r.para_birimi,
                             r.islem_zamani.to_pydatetime(), r.k_topic, int(r.k_partition), int(r.k_offset))
                            for r in temiz.rename(columns=META_ADLARI).itertuples(index=False)])
                        pencereler = sorted({t.floor("min").to_pydatetime() for t in temiz["islem_zamani"]})
                        cur.execute(OZET_YENIDEN, (pencereler,))
                    cur.executemany(OFFSET_UPSERT, [(GROUP, TOPIC, p, o + 1) for p, o in son.items()])
            log.info("yazıldı: temiz=%d red=%d offsetler=%s", len(temiz), len(red), son)
            # Kafka'ya da bilgi amaçlı commit (lag izleme araçları görsün); doğruluk DB'den gelir
            c.commit(offsets=[TopicPartition(TOPIC, p, o + 1) for p, o in son.items()], asynchronous=True)
    finally:
        c.close()
        conn.close()


if __name__ == "__main__":
    main()
