package com.mc.kafka.avro;

import com.mc.kafka.shared.factory.KafkaMessageListenerContainerFactory;
import com.mc.kafka.avro.model.AvroString;
import io.confluent.kafka.schemaregistry.RestApp;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@SpringBootApplication
@Log4j2
public class SimpleAvroProducerConsumerExample implements CommandLineRunner {

    @Autowired
    @Qualifier("topics")
    private String[] topics;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private RestApp schemaRegistry;

    @Autowired
    private KafkaTemplate<String, AvroString> template;

    @Autowired
    private KafkaMessageListenerContainerFactory<AvroString> kafkaMessageListenerContainerFactory;

    public static void main(String... args) {
        SpringApplication app = new SpringApplication(SimpleAvroProducerConsumerExample.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
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

        KafkaMessageListenerContainer<String, AvroString> listenerContainer = kafkaMessageListenerContainerFactory.instance((message) -> {
            log.info("======> Received avro: " + message);
        });

        listenerContainer.start();
        Thread.sleep(5000);


        log.info("======> Starting send...");
        template.send(topic, new AvroString("foo1"));
        template.send(topic, new AvroString("foo2"));
        template.send(topic, new AvroString("foo3"));
        log.info("======> All received.");
    }

}
