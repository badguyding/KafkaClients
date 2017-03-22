package com.iwjw.kafka.clients.consumer;

import com.google.common.collect.Lists;
import com.iwjw.kafka.clients.producer.IWKafkaProducer;

import java.util.concurrent.TimeUnit;

public class IWKafkaProducerTest {

    public static void main(String[] args) throws InterruptedException {

        IWKafkaProducer iwKafkaProducer = new IWKafkaProducer.IWKafkaProducerBuilder()
                .setBrokerServerList(Lists.newArrayList("192.168.1.119:9092","192.168.1.119:9093","192.168.1.119:9094"))
                .build();

        long i = 0;
        while (true) {
            String topic = "testTopic1";
            String key = String.valueOf(i);
            String value = String.valueOf("testMesssage:" + i);
            iwKafkaProducer.sendMessage(topic, key, value);
            i++;
            TimeUnit.MILLISECONDS.sleep(200);
        }

    }
}
