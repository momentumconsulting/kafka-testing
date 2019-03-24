package com.mc.kafka.shared.config;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * The base Spring configuration.
 */
@EnableKafka
public class BaseConfig {

    private static final String[] TOPICS = {"test"};
    private static final int[] KAFKA_BROKER_PORTS = {9092};

    @Bean("topics")
    String[] topics() {
        return TOPICS;
    }

    @Bean
    EmbeddedKafkaBroker embeddedKafkaBroker() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, false, 1, TOPICS);

        broker.kafkaPorts(KAFKA_BROKER_PORTS);

        return broker;
    }

}
