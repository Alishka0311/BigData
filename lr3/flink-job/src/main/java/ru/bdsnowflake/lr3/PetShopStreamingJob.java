package ru.bdsnowflake.lr3;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;

public class PetShopStreamingJob {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy", Locale.US);

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String kafkaTopic = env("KAFKA_TOPIC", "petshop.sales.raw");
        String postgresUrl = env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/pet_shop");
        String postgresUser = env("POSTGRES_USER", "pet_user");
        String postgresPassword = env("POSTGRES_PASSWORD", "pet_password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId("lr3-flink-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<RawSaleEvent> rawEvents = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map((MapFunction<String, RawSaleEvent>) value -> OBJECT_MAPPER.readValue(value, RawSaleEvent.class),
                        TypeInformation.of(RawSaleEvent.class));

        DataStream<CustomerRecord> customerStream = rawEvents
                .map(CustomerRecord::fromEvent, TypeInformation.of(CustomerRecord.class))
                .returns(TypeInformation.of(new TypeHint<CustomerRecord>() {
                }));

        DataStream<SellerRecord> sellerStream = rawEvents
                .map(SellerRecord::fromEvent, TypeInformation.of(SellerRecord.class))
                .returns(TypeInformation.of(new TypeHint<SellerRecord>() {
                }));

        DataStream<PetRecord> petStream = rawEvents
                .map(PetRecord::fromEvent, TypeInformation.of(PetRecord.class))
                .returns(TypeInformation.of(new TypeHint<PetRecord>() {
                }));

        DataStream<ProductRecord> productStream = rawEvents
                .map(ProductRecord::fromEvent, TypeInformation.of(ProductRecord.class))
                .returns(TypeInformation.of(new TypeHint<ProductRecord>() {
                }));

        DataStream<StoreRecord> storeStream = rawEvents
                .map(StoreRecord::fromEvent, TypeInformation.of(StoreRecord.class))
                .returns(TypeInformation.of(new TypeHint<StoreRecord>() {
                }));

        DataStream<SupplierRecord> supplierStream = rawEvents
                .map(SupplierRecord::fromEvent, TypeInformation.of(SupplierRecord.class))
                .returns(TypeInformation.of(new TypeHint<SupplierRecord>() {
                }));

        DataStream<FactSaleRecord> factStream = rawEvents
                .map(FactSaleRecord::fromEvent, TypeInformation.of(FactSaleRecord.class))
                .returns(TypeInformation.of(new TypeHint<FactSaleRecord>() {
                }));

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(postgresUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(postgresUser)
                .withPassword(postgresPassword)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(2_000)
                .withMaxRetries(5)
                .build();

        customerStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_CUSTOMER,
                CustomerRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-customer");

        sellerStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_SELLER,
                SellerRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-seller");

        petStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_PET,
                PetRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-pet");

        productStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_PRODUCT,
                ProductRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-product");

        storeStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_STORE,
                StoreRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-store");

        supplierStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_SUPPLIER,
                SupplierRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-dim-supplier");

        factStream.addSink(JdbcSink.sink(
                SqlStatements.UPSERT_FACT_SALE,
                FactSaleRecord::bind,
                executionOptions,
                connectionOptions
        )).name("postgres-fact-sales");

        env.execute("LR3 Kafka -> Flink -> PostgreSQL");
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    static String stableKey(String prefix, String... values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            String joined = String.join("|", Arrays.stream(values)
                    .map(value -> value == null ? "" : value)
                    .toArray(String[]::new));
            byte[] hash = digest.digest(joined.getBytes(StandardCharsets.UTF_8));
            return prefix + "_" + HexFormat.of().formatHex(hash);
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot create stable key", ex);
        }
    }

    static BigDecimal decimal(String value) {
        return value == null || value.isBlank() ? null : new BigDecimal(value.trim());
    }

    static Integer integer(String value) {
        return value == null || value.isBlank() ? null : Integer.valueOf(value.trim());
    }

    static Long longValue(String value) {
        return value == null || value.isBlank() ? null : Long.valueOf(value.trim());
    }

    static LocalDate date(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(value.trim(), DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }
}
