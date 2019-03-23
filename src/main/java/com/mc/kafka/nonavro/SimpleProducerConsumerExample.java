package com.mc.kafka.nonavro;

import com.mc.kafka.shared.factory.KafkaMessageListenerContainerFactory;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@SpringBootApplication
@Log4j2
public class SimpleProducerConsumerExample implements CommandLineRunner {

    @Autowired
    @Qualifier("topics")
    private String[] topics;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private KafkaMessageListenerContainerFactory kafkaMessageListenerContainerFactory;

    public static void main(String... args) {
        SpringApplication app = new SpringApplication(SimpleProducerConsumerExample.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        String topic = topics[0];

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
