package com.bacebe.bi.job;

import com.bacebe.bi.sink.MongoSink;
import com.bacebe.bi.sink.RechargeMongoSink;
import com.bacebe.bi.source.RechargeSource;
import com.bacebe.bi.source.RocketSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class RechargeJob implements CommandLineRunner {


    @Autowired
    RechargeSource rocketSource;


    @Override
    public void run(String... args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("t", "----------------------->t");
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        DataStreamSource<Object> textStream = streamExecutionEnvironment.addSource(rocketSource);


        SinkFunction sink = new RechargeMongoSink();
        textStream.addSink(sink);


        // 触发任务执行
        streamExecutionEnvironment.execute("RechargeJob");
    }
}
