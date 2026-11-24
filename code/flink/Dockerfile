# Flink 2.3.0 + Kafka SQL connector
# Dosya: code/flink/Dockerfile
# ÖNEMLİ: Connector sürümü Flink sürümüyle uyumlu olmalı. Flink downloads sayfasındaki
# "Apache Flink Kafka Connector" bölümünden Flink 2.3 ile uyumlu jar'ı seçip
# KAFKA_CONNECTOR_URL'i güncelleyin (Maven: org.apache.flink:flink-sql-connector-kafka).
FROM flink:2.3.0
ARG KAFKA_CONNECTOR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar "${KAFKA_CONNECTOR_URL}"
