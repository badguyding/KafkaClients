package com.iwjw.kafka.clients.spring;

import com.iwjw.kafka.clients.consumer.IWKafkaConsumer;
import com.iwjw.kafka.clients.consumer.handler.BusinessMessageHandler;
import org.springframework.beans.factory.FactoryBean;

import java.util.Arrays;

/**
 * <p></p>
 *
 * @author dl
 * @Date 2017/3/22 10:05
 */
public class IWKafkaConsumerFactoryBean implements FactoryBean<IWKafkaConsumer> {
    private String brokerServerList;
    private String consumerGroupId;
    private BusinessMessageHandler businessMessageHandler;
    private String topics;

    @Override
    public IWKafkaConsumer getObject() throws Exception {
        IWKafkaConsumer.IWKafkaConsumerBuilder iwKafkaConsumerBuilder = new IWKafkaConsumer.IWKafkaConsumerBuilder();
        iwKafkaConsumerBuilder.setBrokerServerList(Arrays.asList(brokerServerList.split(",")));
        iwKafkaConsumerBuilder.setConsumerGroupId(consumerGroupId);
        iwKafkaConsumerBuilder.setBusinessMessageHandler(businessMessageHandler);
        iwKafkaConsumerBuilder.setTopics(Arrays.asList(topics.split(",")));
        IWKafkaConsumer iwKafkaConsumer = iwKafkaConsumerBuilder.build();
        iwKafkaConsumer.start();
        return iwKafkaConsumer;
    }

    @Override
    public Class<?> getObjectType() {
        return IWKafkaConsumer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setBrokerServerList(String brokerServerList) {
        this.brokerServerList = brokerServerList;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public void setBusinessMessageHandler(BusinessMessageHandler businessMessageHandler) {
        this.businessMessageHandler = businessMessageHandler;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }
}
