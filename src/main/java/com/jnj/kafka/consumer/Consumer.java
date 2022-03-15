package com.jnj.kafka.consumer;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<Long, GenericRecord> consumer;
    private final String topicName;
    private final Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

    public Consumer(KafkaConsumer<Long, GenericRecord> consumer, String topicName) {
        this.consumer = consumer;
        this.topicName = topicName;
    }

    public void start(long dayOffset, long pollTimeout, boolean logMessages) {

        subscribe(topicName, dayOffset);

        log.info("Consumer started");
        ConsumerRecords<Long, GenericRecord> records;
        try {
            while (!(records = consumer.poll(Duration.ofSeconds(pollTimeout))).isEmpty()) {
                log.info("Consumed {} records", records.count());
                records.forEach(r -> {
                    Long key = r.key();
                    GenericRecord value = r.value();
                    if (value != null) {
                        Map<String, Object> values = getRecordValues(value.getSchema(), value);
                        if (logMessages) {
                            log.info("{}: {}", key, values);
                        }
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error consuming messages", e);
            throw e;
        } finally {
            consumer.close();
        }
        log.info("Consumer finished");
    }

    private void subscribe(String topicName, long dayOffset) {

        long offsetTimestamp = Instant.now().minus(dayOffset, ChronoUnit.DAYS).toEpochMilli();
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
            if ("string".equals(fieldSchema.getName())) {
                if (value instanceof Utf8) {
                    return new String(((Utf8) value).getBytes());
                } else {
                    return value;
                }
            }
        }
        return value;
    }
}
