package com.iwjw.kafka.clients.producer;

import com.google.common.base.Preconditions;
import com.iwjw.kafka.clients.util.BrokerServerUtil;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>消息生产者</p>
 *
 * @author dl
 * @Date 2017/3/22 9:52
 */
public class IWKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(IWKafkaProducer.class);

    private KafkaProducer kafkaProducer;

    public IWKafkaProducer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public boolean sendMessage(final String topic, final String key, final String value) {
        Preconditions.checkState(null != topic && topic.length() != 0, "topic is empty");
        Preconditions.checkState(null != key && key.length() != 0, "key is empty");
        Preconditions.checkState(null != value && value.length() != 0, "value is empty");
        final ProducerRecord producerRecord = new ProducerRecord(topic, key, value);

        final AtomicBoolean sendSuccess = new AtomicBoolean(true);
        final CountDownLatch sendLatch = new CountDownLatch(1);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (null != exception) {
                    logger.error(String.format("send record %s error", producerRecord), exception);
                    sendSuccess.set(false);
                } else {
                    String record = new StringBuilder(50).append("topicPartition[").append(metadata.toString()).append("]")
                            .append("  key[").append(key).append("]")
                            .append("  value[").append(value).append("]").toString();
                    logger.info("send record {} success", record);
                }
                sendLatch.countDown();
            }
        });
        try {
            sendLatch.await();
        } catch (InterruptedException e) {
        }
        return sendSuccess.get();
    }

    public static class IWKafkaProducerBuilder {
        private List<String> brokerServerList;

        public IWKafkaProducerBuilder setBrokerServerList(List<String> brokerServerList) {
            this.brokerServerList = brokerServerList;
            return this;
        }

        public IWKafkaProducer build() {
            BrokerServerUtil.checkBrokerServerList(brokerServerList);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServerUtil.getBrokerServerStr(brokerServerList));
            //This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 1);
            props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            //props.put("unclean.leader.election.enable",true);
            //props.put("min.insync.replicas",1);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(5));
            KafkaProducer kafkaProducer = new KafkaProducer<String, String>(props);
            return new IWKafkaProducer(kafkaProducer);
        }
    }
}
