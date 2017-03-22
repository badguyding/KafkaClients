package com.iwjw.kafka.clients.spring;

import com.iwjw.kafka.clients.producer.IWKafkaProducer;
import org.springframework.beans.factory.FactoryBean;

import java.util.Arrays;

/**
 * <p>IWKafkaProducer FactoryBean</p>
 *
 * @author dl
 * @Date 2017/3/22 10:02
 */
public class IWKafkaProducerFactoryBean implements FactoryBean<IWKafkaProducer> {

    private String brokerServerList;

    @Override
    public IWKafkaProducer getObject() throws Exception {
        IWKafkaProducer.IWKafkaProducerBuilder iwKafkaProducerBuilder = new IWKafkaProducer.IWKafkaProducerBuilder();
        iwKafkaProducerBuilder.setBrokerServerList(Arrays.asList(brokerServerList.split(",")));
        return iwKafkaProducerBuilder.build();
    }

    @Override
    public Class<?> getObjectType() {
        return IWKafkaProducer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setBrokerServerList(String brokerServerList) {
        this.brokerServerList = brokerServerList;
    }
}
