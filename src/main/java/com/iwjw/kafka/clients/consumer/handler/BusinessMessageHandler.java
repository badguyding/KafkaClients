package com.iwjw.kafka.clients.consumer.handler;


import com.iwjw.kafka.clients.consumer.model.MessageObject;

public interface BusinessMessageHandler {

    public void handleMessage(MessageObject messageObject);

    public void close();

}
