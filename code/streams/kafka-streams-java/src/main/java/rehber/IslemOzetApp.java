package rehber;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Streams: islemler.json -> filtre -> islem_tipi bazında 1 dk tumbling pencere toplamı
 * -> islemler.ozet.streams topic'i. Exactly-once (exactly_once_v2) açık.
 * Dosya: code/streams/kafka-streams-java/src/main/java/rehber/IslemOzetApp.java
 */
public class IslemOzetApp {
    private static final ObjectMapper M = new ObjectMapper();

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "rehber-islem-ozet");          // = consumer group + state dizini
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);                        // prod: 3
        p.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-rehber");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> islemler = b.stream("islemler.json");

        islemler
            .mapValues(IslemOzetApp::parse)                                     // mapping
            .filter((k, v) -> v != null && v.has("tutar"))                      // filtering
            .map((k, v) -> KeyValue.pair(v.get("islem_tipi").asText(),          // re-key -> repartition
                                         v.get("tutar").asText()))
            .groupByKey()                                                       // grouping
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(2)))
            .aggregate(() -> "0",
                       (tip, tutar, toplam) -> new BigDecimal(toplam).add(new BigDecimal(tutar)).toPlainString(),
                       Materialized.as("islem-tipi-1dk-toplam"))                 // state store (RocksDB + changelog)
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())) // pencere kapanınca TEK sonuç
            .toStream()
            .map((wk, toplam) -> KeyValue.pair(wk.key() + "@" + wk.window().startTime(), toplam))
            .to("islemler.ozet.streams", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams app = new KafkaStreams(b.build(), p);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> app.close(Duration.ofSeconds(30))));
        app.start();
    }

    private static JsonNode parse(String s) {
        try { return M.readTree(s); } catch (Exception e) { return null; }
    }
}
