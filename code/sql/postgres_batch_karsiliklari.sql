-- =============================================================================
-- Streaming SQL sorgularının PostgreSQL (batch) karşılıkları
-- Dosya: code/sql/postgres_batch_karsiliklari.sql
-- Söz dizimi: PostgreSQL 14+ (date_bin 14 ile geldi). Test: PostgreSQL 16 üzerinde çalıştırıldı.
-- Fark: Bunlar tabloda O ANDA olan veriyi bir kez hesaplar ve biter (bounded).
--       Streaming SQL ise sonsuz akış üzerinde sürekli çalışır ve sonucu günceller (unbounded).
-- =============================================================================

-- Tumbling 1 dk  (ksqlDB: WINDOW TUMBLING (SIZE 1 MINUTE) / Flink: TUMBLE(...))
SELECT date_bin('1 minute', islem_zamani, TIMESTAMPTZ '2000-01-01') AS pencere_baslangic,
       islem_tipi, count(*) AS islem_adedi, sum(tutar) AS toplam_tutar, round(avg(tutar), 2) AS ortalama
FROM public.hesap_hareket
GROUP BY 1, 2
ORDER BY 1, 2;

-- Hopping 5 dk / 1 dk adım: her olay, kapsadığı TÜM pencerelere sayılır
WITH pencereler AS (
  SELECT generate_series(date_trunc('minute', min(islem_zamani)) - interval '4 minutes',
                         date_trunc('minute', max(islem_zamani)),
                         interval '1 minute') AS bas
  FROM public.hesap_hareket
)
SELECT p.bas AS pencere_baslangic, p.bas + interval '5 minutes' AS pencere_bitis,
       h.musteri_id, count(*) AS adet, sum(h.tutar) AS toplam
FROM pencereler p
JOIN public.hesap_hareket h
  ON h.islem_zamani >= p.bas AND h.islem_zamani < p.bas + interval '5 minutes'
GROUP BY 1, 2, 3
ORDER BY 1, 3;

-- Session (30 sn boşluk): "gaps and islands" tekniği
WITH s AS (
  SELECT musteri_id, islem_zamani,
         CASE WHEN islem_zamani - lag(islem_zamani) OVER w > interval '30 seconds'
                OR lag(islem_zamani) OVER w IS NULL THEN 1 ELSE 0 END AS yeni_oturum
  FROM public.hesap_hareket
  WINDOW w AS (PARTITION BY musteri_id ORDER BY islem_zamani)
), o AS (
  SELECT *, sum(yeni_oturum) OVER (PARTITION BY musteri_id ORDER BY islem_zamani) AS oturum_no
  FROM s
)
SELECT musteri_id, oturum_no, min(islem_zamani) AS oturum_baslangic,
       max(islem_zamani) AS son_islem, count(*) AS islem_adedi
FROM o
GROUP BY musteri_id, oturum_no
ORDER BY musteri_id, oturum_no;

-- JOIN (stream-table karşılığı): işlemleri müşteri ile zenginleştir
SELECT h.hareket_id, h.islem_tipi, h.tutar, m.segment, m.sehir
FROM public.hesap_hareket h
LEFT JOIN public.musteri m USING (musteri_id)
ORDER BY h.hareket_id;
