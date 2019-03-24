package com.mc.kafka.avro;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Spring Boot app driver for the {@link SimpleAvroProducerConsumer}.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.mc.kafka.avro", "com.mc.kafka.shared"})
public class SimpleAvroProducerConsumerDriver {

    public static void main(String... args) {
        SpringApplication app = new SpringApplication(SimpleAvroProducerConsumerDriver.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }

}
