package com.mc.kafka.avro;

import com.mc.kafka.avro.model.AvroString;
import com.mc.kafka.shared.factory.KafkaMessageListenerContainerFactory;
import io.confluent.kafka.schemaregistry.RestApp;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.stereotype.Component;

/**
 * A simple producer and consumer example that uses the embedded Kafka broker and Confluent schema registry to produce
 * and consume three {@link AvroString} messages.
 */
@Component
@Log4j2
public class SimpleAvroProducerConsumer implements CommandLineRunner {

    private String[] topics;
    private EmbeddedKafkaBroker broker;
    private RestApp schemaRegistry;
    private KafkaTemplate<String, AvroString> template;
    private KafkaMessageListenerContainerFactory kafkaMessageListenerContainerFactory;

    @Autowired
    public SimpleAvroProducerConsumer(@Qualifier("topics") String[] topics,
                                      EmbeddedKafkaBroker broker,
                                      RestApp schemaRegistry,
                                      KafkaTemplate<String, AvroString> template,
                                      KafkaMessageListenerContainerFactory kafkaMessageListenerContainerFactory) {
        this.topics = topics;
        this.broker = broker;
        this.schemaRegistry = schemaRegistry;
        this.template = template;
        this.kafkaMessageListenerContainerFactory = kafkaMessageListenerContainerFactory;
    }

    @Override
    public void run(String... args) throws Exception {
        String topic = topics[0];

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            broker.destroy();

            try {
                schemaRegistry.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        log.info("======> Starting consumer...");

        KafkaMessageListenerContainer<String, AvroString> listenerContainer =
                kafkaMessageListenerContainerFactory.instance((message) -> {
                    log.info("======> Received avro: " + message);
                });
        listenerContainer.start();

        log.info("Waiting a few seconds...");
        Thread.sleep(5000);

        log.info("======> Starting send...");
        template.send(topic, new AvroString("foo1"));
        template.send(topic, new AvroString("foo2"));
        template.send(topic, new AvroString("foo3"));
        log.info("======> All sent.");
    }

}
