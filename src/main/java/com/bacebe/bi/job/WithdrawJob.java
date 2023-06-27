package com.bacebe.bi.job;

import com.bacebe.bi.sink.MongodbSink;
import com.bacebe.bi.source.RocketSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;


public class WithdrawJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        RocketSource rocketSource=new RocketSource("127.0.0.1",9876,"BI-WITHDRAW","BI-WITHDRAW");
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(rocketSource);
        SinkFunction sink = new MongodbSink("127.0.0.1",27017,"bi","withdraw");
        textStream.addSink(sink);
        // 触发任务执行
        streamExecutionEnvironment.execute("WithdrawJob");
    }
}
