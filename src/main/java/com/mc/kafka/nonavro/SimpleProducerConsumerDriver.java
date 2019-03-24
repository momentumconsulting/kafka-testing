package com.mc.kafka.nonavro;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Spring Boot app driver for the {@link SimpleProducerConsumer}.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.mc.kafka.nonavro", "com.mc.kafka.shared"})
public class SimpleProducerConsumerDriver {

    public static void main(String... args) {
        SpringApplication app = new SpringApplication(SimpleProducerConsumerDriver.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }

}
