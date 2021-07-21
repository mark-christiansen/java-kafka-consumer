package com.machrist.kafka.consumer;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import static java.lang.String.format;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final String ID_FIELD = "id";
    private static final String OPERATION_FIELD = "gwcbi___operation";
    private static final String TIMESTAMP_FIELD = "gwcbi___connector_ts_ms";

    @Autowired
    private KafkaConsumer<GenericRecord, GenericRecord> consumer;
    private Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws ParseException {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(getOptions(), args);

        String topicName = cmd.getOptionValue("topic");
        boolean logValues = cmd.getOptionValue("log") != null && Boolean.parseBoolean(cmd.getOptionValue("log"));
        boolean cda = cmd.getOptionValue("cda") != null && Boolean.parseBoolean(cmd.getOptionValue("cda"));

        Set<String> tableNames = new HashSet<>();
        if (cmd.getOptionValues("tables") != null) {
            tableNames.addAll(Arrays.asList(cmd.getOptionValues("tables")));
        }

        Set<String> sources = new HashSet<>();
        if (cmd.getOptionValues("sources") != null) {
            sources.addAll(Arrays.asList(cmd.getOptionValues("sources")));
        }

        log.info("subscribing to topic {}", topicName);
        subscribe(topicName);
        try {

            Map<String, TableMetrics> tableMetrics = new HashMap<>();
            int totalCount = 0;

            ConsumerRecords<GenericRecord, GenericRecord> records;
            while (!(records = consumer.poll(Duration.ofSeconds(20))).isEmpty()) {
                records.forEach(r -> {

                    GenericRecord key = r.key();
                    Schema keySchema = key.getSchema();

                    String keySchemaName = keySchema.getFullName();
                    String schemaName;
                    // expect something like "uat.raw.cda.bctl_condcmsnsubplantier_ext_key" for cda
                    if (cda && keySchemaName.endsWith("_key")) {
                        schemaName = keySchemaName.substring(0, keySchemaName.length() - 4);
                    // expect something like "com.attunity.queue.msg.chips-qa.TSTDTA.CINMAD.KeyRecord" for non-cda
                    } else if (!cda && keySchemaName.endsWith(".KeyRecord")) {
                        schemaName = keySchemaName.substring(0, keySchemaName.length() - 10);
                    } else {
                        //log.debug(format("Unexpected key schema name %s (cda=%b)", keySchemaName, cda));
                        schemaName = "default";
                       // throw new RuntimeException(format("Unexpected key schema name %s (cda=%b)", keySchemaName, cda));
                    }

                    String tableName = schemaName.substring(schemaName.lastIndexOf(".") + 1);
                    if (tableNames.isEmpty() || tableNames.contains(tableName)) {

                        TableMetrics metrics = tableMetrics.computeIfAbsent(tableName, (k) -> new TableMetrics(tableName));
                        metrics.addMessage();

                        Map<String, Object> keyValues = getRecordValues(keySchema, key);
                        if (cda) {
                            metrics.addId((Long) keyValues.get(ID_FIELD));
                        }

                        GenericRecord value = r.value();
                        if (value != null) {

                            //String sourceSystem = ((GenericRecord)value.get("account")).get("sourceSystem").toString();
                            String sourceSystem = value.get("sourceSystem") != null ? value.get("sourceSystem").toString() : null;
                            if (!sources.isEmpty() && !sources.contains(sourceSystem)) {
                                return;
                            }

                            Map<String, Object> valueValues = getRecordValues(value.getSchema(), value);
                            if (logValues) {
                                log.info("{}", valueValues);
                            }

                            if (cda) {

                                Object operation = valueValues.get(OPERATION_FIELD);
                                if (operation != null) {
                                    switch ((Integer) operation) {
                                        case 0:
                                            metrics.addInitialLoad();
                                            break;
                                        case 1:
                                            metrics.addDelete();
                                            break;
                                        case 2:
                                            metrics.addInsert();
                                            break;
                                        case 4:
                                            metrics.addUpdate();
                                            break;
                                    }
                                }

                            } else {
                                GenericRecord headers = (GenericRecord) value.get("headers");
                                if (headers != null) {
                                    log.info("Table Record [Operation: {}, TransactionId: {}, Timestamp: {}, Change Seq: {}, Event #: {}, Stream Pos: {}]",
                                            headers.get("operation"), headers.get("transactionId"), headers.get("timestamp"), headers.get("changeSequence"),
                                            headers.get("transactionEventCounter"), headers.get("streamPosition"));
                                }
                            }

                        } else {
                            if (logValues) {
                                log.info("Tombstone for key {}", keyValues);
                            }
                        }
                    }
                });
                totalCount += records.count();
                consumer.commitSync();
            }

            log.info("total records consumed={}", totalCount);
            for (String tableName : tableMetrics.keySet()) {
                log.info("{}: {}", tableName, tableMetrics.get(tableName).toString());
            }

            consumer.unsubscribe();
        } finally {
            consumer.close();
        }
    }

    private Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("t").longOpt("topic").optionalArg(false).hasArg(true).desc("Topic to consume").type(String.class).build());
        options.addOption(Option.builder("l").longOpt("log").optionalArg(true).hasArg(true).desc("Log message values to console").type(Boolean.class).build());
        options.addOption(Option.builder("i").longOpt("tables").optionalArg(true).hasArgs().desc("Included tables").type(String.class).build());
        options.addOption(Option.builder("s").longOpt("sources").optionalArg(true).hasArgs().desc("Included sources").type(String.class).build());
        options.addOption(Option.builder("c").longOpt("cda").optionalArg(true).hasArg(true).desc("Consuming CDA topic").type(Boolean.class).build());
        return options;
    }

    private void subscribe(String topicName) {
        long offsetTimestamp = Instant.now().minus(1000, ChronoUnit.DAYS).toEpochMilli();

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
                    if (value instanceof Utf8) {
                        return new String(((Utf8) value).getBytes());
                    } else {
                        return value;
                    }
            }
        }
        return value;
    }

    private static class TableMetrics {

        private final String tableName;
        private int messages;
        private int initialLoads;
        private int inserts;
        private int updates;
        private int deletes;
        private Set<Long> ids = new HashSet<>();

        public TableMetrics(String tableName) {
            this.tableName = tableName;
        }

        public void addMessage() {
            this.messages++;
        }

        public void addInitialLoad() {
            this.initialLoads++;
        }

        public void addInsert() {
            this.inserts++;
        }

        public void addUpdate() {
            this.updates++;
        }

        public void addDelete() {
            this.deletes++;
        }

        public void addId(long id) {
            ids.add(id);
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("messages: ").append(messages);
            sb.append(", initial loads: ").append(initialLoads);
            sb.append(", inserts: ").append(inserts);
            sb.append(", updates: ").append(updates);
            sb.append(", deletes: ").append(deletes);
            sb.append(", ids: ").append(ids.size());
            return sb.toString();
        }
    }
}