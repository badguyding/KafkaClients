package com.iwjw.kafka.clients.spring;

import com.iwjw.kafka.clients.consumer.IWKafkaConsumer;
import com.iwjw.kafka.clients.producer.IWKafkaProducer;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

public class FactoryBeanTest {

	@Test
	public void testProducerFactory(){
		ApplicationContext applicationContext = new GenericXmlApplicationContext("spring-bean.xml");
		IWKafkaProducer iwKafkaProducer = applicationContext.getBean("IWKafkaProducer", IWKafkaProducer.class);
		for (int i = 0; i < 10; i++){
			System.out.println(iwKafkaProducer.sendMessage("A_testTopic", "key-" + i, "this is a test " + i));
		}
	}

	@Test
	public void testConsumerFactory() throws InterruptedException {
		ApplicationContext applicationContext = new GenericXmlApplicationContext("spring-bean.xml");
		IWKafkaConsumer iwKafkaConsumer = applicationContext.getBean("kafkaConsumer", IWKafkaConsumer.class);
		Thread.sleep(10000l);
	}

}
