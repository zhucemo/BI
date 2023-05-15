package com.bacebe.bi.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class RocketSource implements SourceFunction<Object> {
    private Logger logger = LoggerFactory.getLogger(RocketSource.class);

    private static final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TFG");


    private boolean isRunning = true;

    public void run(SourceContext<Object> ctx) throws Exception {
        consumer.setNamesrvAddr("54.249.65.169:9876");
        consumer.subscribe("TFG", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                ctx.collect(new String(msg.getBody()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        while (isRunning) {
            Thread.sleep(5000);
            logger.info("休眠");
        }

    }

    public void cancel() {
        isRunning = false;
        consumer.shutdown();
    }
}