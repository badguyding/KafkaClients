package com.iwjw.kafka.clients.consumer.model;

import com.google.common.base.MoreObjects;

public class MessageObject {

    private final String topic;
    private final String key;
    private final String value;

    public MessageObject(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic)
                .add("key", key)
                .add("value", value)
                .toString();
    }
}
