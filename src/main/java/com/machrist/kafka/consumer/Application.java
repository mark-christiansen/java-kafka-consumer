package com.machrist.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;

@SpringBootApplication
@Slf4j
public class Application implements CommandLineRunner {

    @Autowired
    private KafkaConsumer<GenericRecord, GenericRecord> consumer;

    private Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) {

        String topicName = args[0];
        log.info("topic={}", topicName);
        try {

            long offsetTimestamp = Instant.now().minus(0, ChronoUnit.DAYS).toEpochMilli();

            consumer.subscribe(Collections.singleton(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                    Map<TopicPartition, Long> timestamps = new HashMap<>();
                    for (TopicPartition partition : partitions) {
                        timestamps.put(partition, offsetTimestamp);
                    }

                    Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestamps);
                    for (TopicPartition partition : partitions) {
                        OffsetAndTimestamp offset = offsets.get(partition);
                        if (offset != null) {
                            consumer.seek(partition, offset.offset());
                        }
                    }
                }
            });

            int count = 0;
            ConsumerRecords<GenericRecord, GenericRecord> records;
            while (!(records = consumer.poll(Duration.ofSeconds(20))).isEmpty()) {
                log.info("records returned={}", records.count());
                records.forEach(r -> {

                    GenericRecord key = r.key();
                    Map<String, Object> keyValues = getRecordValues(key.getSchema(), key);
                    log.info("{}", keyValues);

                    GenericRecord value = r.value();
                    if (value != null) {
                        Map<String, Object> valueValues = getRecordValues(value.getSchema(), value);
                        log.info("{}", valueValues);
                    } else {
                        log.info("null");
                    }
                });
                count += records.count();
                consumer.commitSync();
            }
            log.info("total records consumed={}", count);
            consumer.unsubscribe();
        } finally {
            consumer.close();
        }
    }

    private Map<String, Object> getRecordValues(Schema schema, GenericRecord record) {

        Map<String, Object> values = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {

            Schema fieldSchema = field.schema();
            switch (fieldSchema.getName()) {
                case "union":
                    Schema nonNullSchema = fieldSchema.getTypes().get(1);
                    values.put(field.name(), getFieldValue(field.name(), nonNullSchema, record));
                    break;
                case "record":
                    values.put(field.name(), getRecordValues(fieldSchema, (GenericRecord) record.get(field.name())));
                    break;
                default:
                    values.put(field.name(), getFieldValue(field.name(), fieldSchema, record));
            }
        }
        return values;
    }

    private Object getFieldValue(String fieldName, Schema fieldSchema, GenericRecord record) {
        Object value = record.get(fieldName);
        if (value != null) {
            LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType != null) {
                switch (logicalType.getName()) {
                    case "date":
                        return Instant.ofEpochSecond(((Integer) value) * 24 * 3600).atZone(ZoneId.systemDefault()).toLocalDate();
                    case "decimal":
                        return decimalConversion.fromBytes((ByteBuffer) record.get(fieldName), fieldSchema, logicalType);
                    case "timestamp-millis":
                        return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault()).toLocalDateTime();
                    default:
                }
            }
            switch (fieldSchema.getName()) {
                case "string":
                    return new String(((Utf8) value).getBytes());
            }
        }
        return value;
    }
}