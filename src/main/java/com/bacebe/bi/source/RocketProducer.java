package com.bacebe.bi.source;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class RocketProducer {
    private Logger logger = LoggerFactory.getLogger(RocketProducer.class);

    private static DefaultMQPushConsumer consumer;

    private String host,topic,group;
    private Integer port;

    private final DefaultMQProducer producer;

    public RocketProducer(String host, int port, String topic) throws MQClientException {
        this.host=host;
        this.port=port;
        this.topic=topic;
        DefaultMQProducer producer = new DefaultMQProducer("BI_GROUP");
        producer.setNamesrvAddr(host + ":" + port);
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        this.producer = producer;
    }

    public void send(String jsonObject) throws Exception {

        Message msg = new Message(topic,
                topic,
                jsonObject.getBytes(RemotingHelper.DEFAULT_CHARSET));

        producer.send(msg);

    }

}