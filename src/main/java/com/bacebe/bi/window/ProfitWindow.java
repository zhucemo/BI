package com.bacebe.bi.window;

import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.source.RocketProducer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.client.exception.MQClientException;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

public class ProfitWindow  implements WindowFunction<Tuple2<String, BigDecimal>, String, String, TimeWindow> {


    private final RocketProducer rocketProducer;

    public ProfitWindow(String host, int port, String topic) throws MQClientException {


        this.rocketProducer = new RocketProducer(host, port, topic);
    }


    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, BigDecimal>> input, Collector<String> out) throws Exception {
        BigDecimal sum = BigDecimal.ZERO;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        for(Tuple2<String,BigDecimal> tuple2 : input){
            sum = sum.add(tuple2.f1);
        }
        JSONObject profitPojo = new JSONObject();
        profitPojo.put("address", key);
        profitPojo.put("profit", sum);
        rocketProducer.send(profitPojo.toJSONString());
        long start = window.getStart();
        long end = window.getEnd();


        out.collect("key:" + key + " value: " + sum + "| window_start :"
                + format.format(start) + "  window_end :" + format.format(end)
        );
    }
}
