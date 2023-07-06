package com.bacebe.bi.sink;



import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.math.BigDecimal;


@Slf4j
public class RocketSink extends RichSinkFunction<Tuple2<String, BigDecimal>> {


    private DefaultMQProducer producer;

    private String host,topic;
    private Integer port;

    public RocketSink(String host, int port, String topic){
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
    public void invoke(Tuple2<String, BigDecimal> bean, Context context) throws Exception {
        JSONObject profitPojo = new JSONObject();
        profitPojo.put("address", bean.f0);
        profitPojo.put("profit", bean.f1);
        Message msg = new Message(topic,
                topic,
                JSON.toJSONBytes(profitPojo));
        log.info("sink profit----------> {}", bean);
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