package com.bacebe.bi.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class RocketSource implements SourceFunction<String> {
    private Logger logger = LoggerFactory.getLogger(RocketSource.class);

    private static DefaultMQPushConsumer consumer;

    private String host,topic,group;
    private Integer port;

    public RocketSource(String host, int port, String topic, String group){
        this.host=host;
        this.port=port;
        this.topic=topic;
        this.group=group;
    }

    private boolean isRunning = true;

    public void run(SourceContext<String> ctx) throws Exception {
        this.consumer= new DefaultMQPushConsumer(group);
        System.out.println(host+":"+port);
        consumer.setNamesrvAddr(host+":"+port);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                String data = new String(msg.getBody());
                log.info("receive msg:" + data);
                ctx.collect(data);
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        while (isRunning) {
            Thread.sleep(5000);
        }

    }

    public void cancel() {
        isRunning = false;
        consumer.shutdown();
    }
}