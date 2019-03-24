package com.mc.kafka.shared.factory;

import com.mc.kafka.shared.model.KafkaProps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

/**
 * A Factory used to instantiate a {@link KafkaMessageListenerContainer} with a configured {@link MessageListener}.
 */
@Component
public class KafkaMessageListenerContainerFactory {

    private KafkaProps kafkaProps;

    @Autowired
    public KafkaMessageListenerContainerFactory(@Qualifier("consumerProps") KafkaProps kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    public <Value> KafkaMessageListenerContainer<String, Value> instance(MessageListener<String, Value> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(kafkaProps.getTopics());
        DefaultKafkaConsumerFactory<String, Value> defaultKafkaConsumerFactory =
                new DefaultKafkaConsumerFactory<>(kafkaProps.getProps());

        containerProperties.setMessageListener(messageListener);

        return new KafkaMessageListenerContainer<>(defaultKafkaConsumerFactory, containerProperties);
    }

}
