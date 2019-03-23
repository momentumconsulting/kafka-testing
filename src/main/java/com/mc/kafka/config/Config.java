package com.mc.kafka.config;

import com.mc.kafka.KafkaProps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class Config {

    private static final String[] TOPICS = {"test"};

    public static final int[] PORTS = {9092};

    @Bean("topics")
    String[] topics() {
        return TOPICS;
    }

    @Bean
    EmbeddedKafkaBroker embeddedKafkaBroker() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, true, 1, TOPICS);

        broker.kafkaPorts(PORTS);

        return broker;
    }

    @Bean
    KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<String, String> pf =
                new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
        return template;
    }

    @Bean("consumerProps")
    KafkaProps kafkaConsumerProps() {
        return new KafkaProps(consumerProps(), topics());
    }

    Map<String, Object> consumerProps() {
        EmbeddedKafkaBroker broker = embeddedKafkaBroker();

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

    Map<String, Object> senderProps() {
        EmbeddedKafkaBroker broker = embeddedKafkaBroker();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

}
