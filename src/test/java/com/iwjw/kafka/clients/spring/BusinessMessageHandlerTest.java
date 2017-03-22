package com.iwjw.kafka.clients.spring;


import com.iwjw.kafka.clients.consumer.handler.BusinessMessageHandler;
import com.iwjw.kafka.clients.consumer.model.MessageObject;

public class BusinessMessageHandlerTest implements BusinessMessageHandler {

    @Override
    public void handleMessage(MessageObject messageObject) {
        System.out.println("=============>" + messageObject);
    }

    @Override
    public void close() {

    }

}
