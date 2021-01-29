package com.machrist.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
public class Config {

    @Bean
    @ConfigurationProperties(prefix = "application.consumer")
    public Properties consumerProperties() {
        return new Properties();
    }

    @Bean
    public KafkaConsumer kafkaConsumer() {
        Properties props = consumerProperties();
        System.setProperty("javax.net.ssl.keyStore", (String) props.get("ssl.keystore.location"));
        System.setProperty("javax.net.ssl.keyStorePassword", (String) props.get("ssl.keystore.password"));
        System.setProperty("javax.net.ssl.trustStore", (String) props.get("ssl.truststore.location"));
        System.setProperty("javax.net.ssl.trustStorePassword", (String) props.get("ssl.truststore.password"));
        return new KafkaConsumer<>(consumerProperties());
    }
}
