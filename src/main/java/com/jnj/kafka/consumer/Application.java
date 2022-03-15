package com.jnj.kafka.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final String OPTION_TOPIC = "topic";
    private static final String OPTION_DAY_OFFSET = "days";
    private static final String OPTION_POLL_TIMEOUT = "timeout";
    private static final long DEFAULT_POLL_TIMEOUT = 5;
    private static final String OPTION_LOG_MESSAGES = "log";

    @Autowired
    private KafkaConsumer<Long, GenericRecord> kafkaConsumer;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    @Override
    public void run(String... args) throws Exception {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(getOptions(), args);
        String topicName = cmd.getOptionValue(OPTION_TOPIC);
        long dayOffset = Long.parseLong(cmd.getOptionValue(OPTION_DAY_OFFSET));
        long pollTimeout = cmd.hasOption(OPTION_POLL_TIMEOUT) ? Long.parseLong(cmd.getOptionValue(OPTION_POLL_TIMEOUT)) :
                DEFAULT_POLL_TIMEOUT;
        boolean log = Boolean.parseBoolean(cmd.getOptionValue(OPTION_LOG_MESSAGES));
        Consumer consumer = new Consumer(kafkaConsumer, topicName);
        consumer.start(dayOffset, pollTimeout, log);
    }

    private Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("t").longOpt(OPTION_TOPIC).optionalArg(false)
                .hasArg(true).desc("Topic to consume from").type(String.class).build());
        options.addOption(Option.builder("d").longOpt(OPTION_DAY_OFFSET).optionalArg(false)
                .hasArg(true).desc("Days to go backwards for consumer offset start").type(Long.class).build());
        options.addOption(Option.builder("p").longOpt(OPTION_POLL_TIMEOUT).optionalArg(true)
                .hasArg(true).desc("Maximum seconds to wait for messages until quitting").type(Long.class).build());
        options.addOption(Option.builder("l").longOpt(OPTION_LOG_MESSAGES).optionalArg(true)
                .hasArg(true).desc("Log message values to console").type(Boolean.class).build());
        return options;
    }
}
