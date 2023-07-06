package com.bacebe.bi.sink;



import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.math.BigDecimal;


@Slf4j
public class RocketStringSink extends RichSinkFunction<String> {


    private DefaultMQProducer producer;

    private String host,topic;
    private Integer port;

    public RocketStringSink(String host, int port, String topic){
        this.host=host;
        this.port=port;
        this.topic=topic;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DefaultMQProducer producer = new DefaultMQProducer("BI_GROUP");
        producer.setNamesrvAddr(host + ":" + port);
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        this.producer = producer;
    }

    @Override
    public void invoke(String bean, Context context) throws Exception {

        Message msg = new Message(topic,
                topic, bean.getBytes());
        producer.send(msg);
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (null != producer) {
                System.out.println("rocket 关闭啦！");
            }
        } catch (Exception e) {
    //           e.printStackTrace();
        }
    }
}