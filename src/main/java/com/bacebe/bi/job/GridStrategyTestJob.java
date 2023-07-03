package com.bacebe.bi.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.model.StrategyDocument;
import com.bacebe.bi.sink.MongodbUpsertSink;
import com.bacebe.bi.source.RocketSource;
import com.bacebe.bi.source.TestSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;


@Slf4j
public class GridStrategyTestJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(new TestSource());
        SingleOutputStreamOperator<StrategyDocument> strategyDocumentDataStreamSource = textStream.map((String value) -> {
            int i = 1 / 0;
            log.info(String.valueOf(i));
            return new StrategyDocument();
        }).returns(TypeInformation.of(StrategyDocument.class)).name("映射网格");
        SinkFunction sink = new MongodbUpsertSink("127.0.0.1",27017,"bi","strategy");
        strategyDocumentDataStreamSource.addSink(sink);
        // 触发任务执行
        streamExecutionEnvironment.execute("gridJob");
    }
}
