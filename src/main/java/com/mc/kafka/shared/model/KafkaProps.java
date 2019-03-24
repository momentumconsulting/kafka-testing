package com.mc.kafka.shared.model;

import lombok.Data;

import java.util.Map;

/**
 * A model that contains Kafka property information.
 */
@Data
public class KafkaProps {

    private Map<String, Object> props;

    private String[] topics;

    public KafkaProps(Map<String, Object> props, String[] topics) {
        this.props = props;
        this.topics = topics;
    }

}
