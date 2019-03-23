package com.mc.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageListenerContainerFactory {

    private KafkaProps kafkaProps;

    @Autowired
    public KafkaMessageListenerContainerFactory(@Qualifier("consumerProps") KafkaProps kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    public KafkaMessageListenerContainer<String, String> instance(MessageListener<String, String> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(kafkaProps.getTopics());
        DefaultKafkaConsumerFactory<String, String> defaultKafkaConsumerFactory =
                new DefaultKafkaConsumerFactory<>(kafkaProps.getProps());

        containerProperties.setMessageListener(messageListener);

        return new KafkaMessageListenerContainer<>(defaultKafkaConsumerFactory, containerProperties);
    }

}
