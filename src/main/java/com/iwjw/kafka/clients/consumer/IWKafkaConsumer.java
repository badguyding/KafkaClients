package com.iwjw.kafka.clients.consumer;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.iwjw.kafka.clients.consumer.handler.BusinessMessageHandler;
import com.iwjw.kafka.clients.consumer.model.MessageObject;
import com.iwjw.kafka.clients.util.BrokerServerUtil;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>消息消费者</p>
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Detailed+Consumer+Coordinator+Design
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread
 *
 * @author dl
 * @Date 2017/3/22 10:07
 */
public class IWKafkaConsumer {

    private static volatile int IWKafkaConsumerInstanceCount = 0;
    private static volatile int commitOffsetRequestLimit = 1 << 15;
    private static volatile int businessRingBufferLimit = 1 << 20;

    private ArrayBlockingQueue<CommitOffsetRequest> commitOffsetRequestList = new ArrayBlockingQueue<CommitOffsetRequest>(commitOffsetRequestLimit);

    private static final Logger logger = LoggerFactory.getLogger(IWKafkaConsumer.class);

    private KafkaConsumer<String, String> kafkaConsumer;
    private BusinessMessageHandler businessMessageHandler;
    private Disruptor<MessageObjectEvent> businessConsumerDisruptor;

    private ExecutorService businessConsumerExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "businessConsumer");
        }
    });
    private volatile boolean stopped;
    private CountDownLatch stopLatch = new CountDownLatch(1);

    public IWKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer, BusinessMessageHandler businessMessageHandler) {
        if (++IWKafkaConsumerInstanceCount >= Integer.parseInt(System.getProperty("IWKafkaConsumerInstanceLimit", "2"))) {
            throw new RuntimeException(String.format("IWKafkaConsumerInstanceCount current  %s large than limit %s", IWKafkaConsumerInstanceCount, Integer.parseInt(System.getProperty("IWKafkaConsumerInstanceLimit", "2"))));
        }
        this.kafkaConsumer = kafkaConsumer;
        this.businessMessageHandler = businessMessageHandler;
        businessConsumerDisruptor = new Disruptor<MessageObjectEvent>(new EventFactory<MessageObjectEvent>() {
            @Override
            public MessageObjectEvent newInstance() {
                return new MessageObjectEvent();
            }
        }, businessRingBufferLimit, businessConsumerExecutor, ProducerType.SINGLE, new BlockingWaitStrategy());
    }

    public void start() {
        startBusinessConsumer();
        startKafkaPull();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                IWKafkaConsumer.this.close();
            }
        }));
        logger.info("start IWKafkaConsumer success");
    }

    public void close() {
        stopBusinessConsumer();
        stopKafkaPoll();
    }

    private void startBusinessConsumer() {
        businessConsumerDisruptor.handleEventsWith(new EventHandler<MessageObjectEvent>() {
            @Override
            public void onEvent(MessageObjectEvent messageObjectEvent, long l, boolean b) throws Exception {
                try {
                    logger.info("receive msg {}", messageObjectEvent);
                    businessMessageHandler.handleMessage(messageObjectEvent.getMessageObject());
                    commitOffsetRequestList.put(new CommitOffsetRequest(messageObjectEvent.getTopic(), messageObjectEvent.getPartition(), messageObjectEvent.getOffset() + 1));
                } catch (Throwable throwable) {
                    logger.error("consumer message error", throwable);
                } finally {
                    messageObjectEvent.free();
                }
            }
        });
        businessConsumerDisruptor.start();
    }

    private void startKafkaPull() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopped) {
                    try {
                        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(2000);
                        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                            final String topic = consumerRecord.topic();
                            final String key = consumerRecord.key();
                            final String value = consumerRecord.value();
                            final int partition = consumerRecord.partition();
                            final long offset = consumerRecord.offset();
                            businessConsumerDisruptor.publishEvent(new EventTranslator<MessageObjectEvent>() {
                                @Override
                                public void translateTo(MessageObjectEvent event, long sequence) {
                                    event.setMessageObject(new MessageObject(topic, key, value));
                                    event.setPartition(partition);
                                    event.setOffset(offset);
                                }
                            });
                        }
                        commitConsumerOffset();
                    } catch (Throwable throwable) {
                        if (throwable instanceof CommitFailedException) {
                            logger.error("commit failure", throwable);
                        } else {
                            logger.error("kafka bug", throwable);
                        }
                    }
                }
                stopLatch.countDown();
            }
        }, "kafkaPull").start();
    }

    private void stopBusinessConsumer() {
        logger.info("begin stopBusinessConsumer");
        this.stopped = true;
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            logger.error(String.format("currentThread %s is interrupted"), Thread.currentThread().getName());
        }
        businessConsumerDisruptor.shutdown();
        MoreExecutors.shutdownAndAwaitTermination(businessConsumerExecutor, 1, TimeUnit.MINUTES);
        commitConsumerOffset();
        businessMessageHandler.close();
        logger.info("end stopBusinessConsumer");
    }

    private void stopKafkaPoll() {
        logger.info("begin stopKafkaPoll");
        kafkaConsumer.close();
        logger.info("end stopKafkaPoll");
    }

    private void commitConsumerOffset() {
        if (!commitOffsetRequestList.isEmpty()) {
            List<CommitOffsetRequest> temporaryCommitOffsetRequestList = Lists.newArrayList();
            commitOffsetRequestList.drainTo(temporaryCommitOffsetRequestList);

            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = Maps.newHashMap();
            for (CommitOffsetRequest temporaryCommitOffset : temporaryCommitOffsetRequestList) {
                TopicPartition topicPartition = new TopicPartition(temporaryCommitOffset.getTopic(), temporaryCommitOffset.getPartition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(temporaryCommitOffset.getOffset());
                topicPartitionOffsetAndMetadataMap.putIfAbsent(topicPartition, offsetAndMetadata);
                if (topicPartitionOffsetAndMetadataMap.get(topicPartition).offset() < offsetAndMetadata.offset()) {
                    topicPartitionOffsetAndMetadataMap.put(topicPartition, offsetAndMetadata);
                }
            }
            logger.info("commit consumer offset {}", topicPartitionOffsetAndMetadataMap);
            kafkaConsumer.commitSync(topicPartitionOffsetAndMetadataMap);
        }
    }

    public static class IWKafkaConsumerBuilder {
        private List<String> brokerServerList;
        private String consumerGroupId;
        private BusinessMessageHandler businessMessageHandler;
        private List<String> topics;

        public IWKafkaConsumerBuilder setBrokerServerList(List<String> brokerServerList) {
            this.brokerServerList = brokerServerList;
            return this;
        }

        public IWKafkaConsumerBuilder setConsumerGroupId(String consumerGroupId) {
            this.consumerGroupId = consumerGroupId;
            return this;
        }

        public IWKafkaConsumerBuilder setBusinessMessageHandler(BusinessMessageHandler businessMessageHandler) {
            this.businessMessageHandler = businessMessageHandler;
            return this;
        }

        public IWKafkaConsumerBuilder setTopics(List<String> topics) {
            this.topics = topics;
            return this;
        }

        public IWKafkaConsumer build() {
            if (null == brokerServerList || brokerServerList.size() == 0) {
                throw new IllegalArgumentException("brokerServerList is empty");
            }
            if (null == topics || topics.size() == 0) {
                throw new IllegalArgumentException("topics is empty");
            }
            if (null == consumerGroupId || consumerGroupId.isEmpty()) {
                throw new IllegalArgumentException("consumerGroupId is empty");
            }
            if (null == businessMessageHandler) {
                throw new IllegalArgumentException("businessMessageHandler is null");
            }
            BrokerServerUtil.checkBrokerServerList(brokerServerList);
            {
                Map<String, Object> configs = new HashMap<>();
                configs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
                configs.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerGroupId + "-consumer" + getCurrentPId());
                configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServerUtil.getBrokerServerStr(brokerServerList));
                configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

                //一次poll 最大条数
                configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
                //心跳超时时间
                configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(10));
                //发送心跳间隔
                configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(3));
                //允许业务逻辑处理最大耗时
                configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) TimeUnit.MINUTES.toMillis(5));
                configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                KafkaConsumer kafkaConsumer = new KafkaConsumer<>(configs);
                kafkaConsumer.subscribe(topics);
                return new IWKafkaConsumer(kafkaConsumer, businessMessageHandler);
            }
        }

        private String getCurrentPId() {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            return name.split("@")[0];
        }
    }

    private static class MessageObjectEvent {
        private MessageObject messageObject;
        private int partition;
        private long offset;

        public MessageObjectEvent() {
        }

        public MessageObject getMessageObject() {
            return messageObject;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setMessageObject(MessageObject messageObject) {
            this.messageObject = messageObject;
        }

        public String getTopic() {
            return messageObject.getTopic();
        }

        public void free() {
            messageObject = null;
            partition = Integer.MIN_VALUE;
            offset = Integer.MIN_VALUE;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("partition", partition)
                    .add("offset", offset)
                    .add("topic", messageObject.getTopic())
                    .add("key", messageObject.getKey())
                    .add("value", messageObject.getValue())
                    .toString();
        }
    }

    private static class CommitOffsetRequest {
        private final int partition;
        private final String topic;
        private final long offset;

        public CommitOffsetRequest(String topic, int partition, long offset) {
            this.offset = offset;
            this.topic = topic;
            this.partition = partition;
        }

        public int getPartition() {
            return partition;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }
    }
}
