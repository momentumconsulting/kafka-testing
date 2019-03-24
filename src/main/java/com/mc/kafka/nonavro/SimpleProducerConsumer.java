package com.mc.kafka.nonavro;

import com.mc.kafka.shared.factory.KafkaMessageListenerContainerFactory;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.stereotype.Component;

/**
 * A simple producer and consumer example that uses the embedded Kafka broker to produce and consume three
 * {@link String} messages.
 */
@Component
@Log4j2
public class SimpleProducerConsumer implements CommandLineRunner {

    private String topic;
    private EmbeddedKafkaBroker broker;
    private KafkaTemplate<String, String> template;
    private KafkaMessageListenerContainerFactory kafkaMessageListenerContainerFactory;

    @Autowired
    public SimpleProducerConsumer(@Qualifier("topics") String[] topics,
                                  EmbeddedKafkaBroker broker,
                                  KafkaTemplate<String, String> template,
                                  KafkaMessageListenerContainerFactory kafkaMessageListenerContainerFactory) {
        this.topic = topics[0];
        this.broker = broker;
        this.template = template;
        this.kafkaMessageListenerContainerFactory = kafkaMessageListenerContainerFactory;
    }

    @Override
    public void run(String... args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            broker.destroy();
        }));

        log.info("======> Starting consumer...");

        KafkaMessageListenerContainer<String, String> listenerContainer = kafkaMessageListenerContainerFactory.instance((message) -> {
            log.info("======> Received: " + message);
        });
        listenerContainer.start();

        log.info("Waiting a few seconds...");
        Thread.sleep(5000);

        log.info("======> Starting send...");
        template.send(topic, "foo1");
        template.send(topic, "foo2");
        template.send(topic, "foo3");
        log.info("======> All sent.");
    }
}
