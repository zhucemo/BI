package com.bacebe.bi.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.model.StrategyDocument;
import com.bacebe.bi.sink.MongodbUpsertSink;
import com.bacebe.bi.source.RocketSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.HashMap;


public class GridStrategyJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        RocketSource rocketSource=new RocketSource("127.0.0.1",9876,"BI_GRID","BI_GRID");
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(rocketSource);
        SingleOutputStreamOperator<StrategyDocument> strategyDocumentDataStreamSource = textStream.flatMap((FlatMapFunction<String, StrategyDocument>) (value, out) -> {
            JSONObject jsonObject = JSON.parseObject(value);
            StrategyDocument strategyDocument = new StrategyDocument();
            strategyDocument.setId("200_" + jsonObject.getLong("id"));
            strategyDocument.setAddress(jsonObject.getString("address"));
            strategyDocument.setFees(new JSONObject());
            strategyDocument.getFees().put(jsonObject.getString("coin"), jsonObject.getBigDecimal("coinFee").toPlainString());
            strategyDocument.getFees().put(jsonObject.getString("goods"), jsonObject.getBigDecimal("goodsFee").toPlainString());
            strategyDocument.setAddressType(100);
            strategyDocument.setEndTime(jsonObject.getDate("endTime"));
            strategyDocument.setStatus(jsonObject.getInteger("status"));
            strategyDocument.setAviCoinAmount(jsonObject.getBigDecimal("profitCoin").subtract(jsonObject.getBigDecimal("extractedCoin")).toPlainString());
            strategyDocument.setUnsoldCoinAmount(jsonObject.getBigDecimal("coinAmount").subtract(jsonObject.getBigDecimal("profitCoin")).toPlainString());
            BigDecimal cumulativeUsageAmount = jsonObject.getBigDecimal("oriAmount").subtract(jsonObject.getBigDecimal("coinAmount").subtract(jsonObject.getBigDecimal("profitCoin")));
            strategyDocument.setCumulativeUsageAmount(cumulativeUsageAmount.toPlainString());
            strategyDocument.setStartTime(jsonObject.getDate("createdAt"));
            strategyDocument.setOriCoinAmount(jsonObject.getBigDecimal("oriAmount").toPlainString());
            strategyDocument.setType(200);
            out.collect(strategyDocument);
        });
        SinkFunction sink = new MongodbUpsertSink("127.0.0.1",27017,"bi","strategy");
        strategyDocumentDataStreamSource.addSink(sink);
        // 触发任务执行
        streamExecutionEnvironment.execute("gridJob");
    }
}
