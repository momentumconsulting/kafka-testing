package com.mc.kafka.avro;

import com.mc.kafka.shared.model.KafkaProps;
import com.mc.kafka.shared.config.BaseConfig;
import com.mc.kafka.avro.model.AvroString;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A Spring configuration for the Avro producer and consumer.
 */
@Configuration
@Import(BaseConfig.class)
public class ConfigAvro {

    private static final int SCHEMA_REG_PORT = 19092;
    private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "10000";
    private static final String KAFKASTORE_DEBUG = "true";
    private static final String KAFKASTORE_INIT_TIMEOUT = "90000";
    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

    @Bean
    RestApp schemaRegistry(EmbeddedKafkaBroker broker) throws Exception {
        final Properties props = new Properties();

        props.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
        props.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
        props.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);
        props.put(SchemaRegistryConfig.HOST_NAME_CONFIG, "localhost");

        RestApp schemaRegistry = new RestApp(SCHEMA_REG_PORT, broker.getZookeeperConnectionString(),
                KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE, props);
        schemaRegistry.start();

        return schemaRegistry;
    }

    @Bean
    KafkaTemplate<String, AvroString> kafkaTemplate(EmbeddedKafkaBroker broker, RestApp schemaRegistry) {
        Map<String, Object> senderProps = producerProps(broker, schemaRegistry);
        ProducerFactory<String, AvroString> pf =
                new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<String, AvroString> template = new KafkaTemplate<>(pf);

        return template;
    }

    @Bean("consumerProps")
    KafkaProps kafkaConsumerProps(EmbeddedKafkaBroker broker,
                                  RestApp schemaRegistry,
                                  @Qualifier("topics") String[] topics) {
        return new KafkaProps(consumerProps(broker, schemaRegistry), topics);
    }

    Map<String, Object> consumerProps(EmbeddedKafkaBroker broker, RestApp schemaRegistry) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.masterIdentity().getUrl());

        return props;
    }

    Map<String, Object> producerProps(EmbeddedKafkaBroker broker, RestApp schemaRegistry) {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.masterIdentity().getUrl());

        return props;
    }

}
