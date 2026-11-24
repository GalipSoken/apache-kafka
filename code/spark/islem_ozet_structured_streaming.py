"""
Spark Structured Streaming: Kafka -> 1 dk tumbling pencere (watermark 2 dk) -> konsol / Kafka
Dosya: code/spark/islem_ozet_structured_streaming.py
Sürüm: Apache Spark 4.x (PySpark). Kafka connector paketi Spark sürümüyle AYNI olmalı:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:<SPARK_SURUMU> islem_ozet_structured_streaming.py
"""
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("rehber-islem-ozet")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

sema = T.StructType([
    T.StructField("hareket_id", T.LongType()),
    T.StructField("musteri_id", T.LongType()),
    T.StructField("islem_tipi", T.StringType()),
    T.StructField("tutar", T.StringType()),          # JSON'da string taşınıyor
    T.StructField("islem_ts_ms", T.LongType()),
])

ham = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "islemler.json")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 50000)        # micro-batch başına üst sınır (backpressure)
       .load())

olaylar = (ham.select(F.from_json(F.col("value").cast("string"), sema).alias("o"))
           .select("o.*")
           .withColumn("tutar", F.col("tutar").cast(T.DecimalType(18, 2)))
           .withColumn("olay_zamani", F.timestamp_millis("islem_ts_ms")))

ozet = (olaylar
        .withWatermark("olay_zamani", "2 minutes")                       # geç veri toleransı + state temizliği
        .groupBy(F.window("olay_zamani", "1 minute"), "islem_tipi")
        .agg(F.count("*").alias("adet"), F.sum("tutar").alias("toplam")))

sorgu = (ozet.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", False)
         .option("checkpointLocation", "/tmp/spark-chk/islem-ozet")      # offset + state kurtarma
         .trigger(processingTime="10 seconds")
         .start())
sorgu.awaitTermination()
