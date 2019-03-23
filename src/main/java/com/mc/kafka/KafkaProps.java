package com.mc.kafka;

import lombok.Data;

import java.util.Map;

@Data
public class KafkaProps {

    private Map<String, Object> props;

    private String[] topics;

    public KafkaProps(Map<String, Object> props, String[] topics) {
        this.props = props;
        this.topics = topics;
    }

}
