package com.bacebe.bi.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.sink.MongodbSink;
import com.bacebe.bi.sink.RocketSink;
import com.bacebe.bi.sink.RocketStringSink;
import com.bacebe.bi.source.RocketSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class ChainLogJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        RocketSource rocketSource=new RocketSource("127.0.0.1",9876,"BI_CHAIN_LOG","BI_CHAIN_LOG");
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(rocketSource);

        SingleOutputStreamOperator<String> operator = textStream.flatMap((String value, Collector<String> out) -> {
            JSONObject jsonObject = JSON.parseObject(value);
            int function = jsonObject.getIntValue("function");
            if (function == 1200 || function == 1300 || function == 1000 || function == 600 || function == 700) {
                out.collect(value);
            }
        }).returns(Types.STRING).name("gas 预警数据收集");

        SinkFunction sink = new MongodbSink("127.0.0.1",27017,"bi","chain_log");
        textStream.addSink(sink);
        operator.addSink(new RocketStringSink("127.0.0.1",9876,"SYSTEM_CHAIN_GAS"));
        // 触发任务执行
        streamExecutionEnvironment.execute("ChainLogJob");
    }
}
