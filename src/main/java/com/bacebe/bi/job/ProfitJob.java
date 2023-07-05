package com.bacebe.bi.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bacebe.bi.sink.RocketSink;
import com.bacebe.bi.source.RocketSource;
import com.bacebe.bi.window.ProfitWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.math.BigDecimal;
import java.util.HashMap;


@Slf4j
public class ProfitJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        RocketSource rocketSource = new RocketSource("127.0.0.1",9876,"BI_PROFIT","BI_PROFIT");
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(rocketSource);
        SingleOutputStreamOperator<Tuple2<String, BigDecimal>> singleOutputStreamOperator = textStream.map((String value) -> {
            JSONObject jsonObject = JSON.parseObject(value);
            log.info("profit:{}", jsonObject);
            return new Tuple2<> (jsonObject.getString("address"), jsonObject.getBigDecimal("profit"));
        }).returns(Types.TUPLE(Types.STRING, TypeInformation.of(BigDecimal.class)));
        KeyedStream<Tuple2<String, BigDecimal>, String> tuple2StringKeyedStream = singleOutputStreamOperator.keyBy((KeySelector<Tuple2<String, BigDecimal>, String>) value -> value.getField(0));
        WindowedStream<Tuple2<String, BigDecimal>, String, TimeWindow> window = tuple2StringKeyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));
        window.apply(new ProfitWindow()).addSink(new RocketSink("127.0.0.1",9876,"SYSTEM_PROFIT_SLID"));
        // 触发任务执行
        streamExecutionEnvironment.execute("ProfitJob");
    }
}
