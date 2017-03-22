package com.iwjw.kafka.clients.consumer;

import com.google.common.collect.Lists;
import com.iwjw.kafka.clients.consumer.handler.BusinessMessageHandler;
import com.iwjw.kafka.clients.consumer.model.MessageObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class IWKafkaConsumerTest {

    public static void main(String[] args) throws Exception {

        BusinessMessageHandler businessMessageHandler = new BusinessMessageHandler() {
            @Override
            public void handleMessage(MessageObject messageObject) {
                System.out.println(messageObject);
            }

            @Override
            public void close() {
            }
        };


        IWKafkaConsumer iwKafkaConsumer = new IWKafkaConsumer.IWKafkaConsumerBuilder().setBrokerServerList(Lists.newArrayList("192.168.1.119:9092","192.168.1.119:9093","192.168.1.119:9094"))
                .setConsumerGroupId("consumerGroup")
                .setBusinessMessageHandler(businessMessageHandler)
                .setTopics(Lists.newArrayList("testTopic1"))
                .build();
        iwKafkaConsumer.start();


        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        while (bufferedReader.readLine().equals("exit")) {
            System.exit(0);
        }
    }

}
