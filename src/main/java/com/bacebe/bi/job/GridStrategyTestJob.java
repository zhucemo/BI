package com.bacebe.bi.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.model.StrategyDocument;
import com.bacebe.bi.sink.MongodbUpsertSink;
import com.bacebe.bi.source.RocketSource;
import com.bacebe.bi.source.TestSource;
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


public class GridStrategyTestJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(new TestSource());
        SingleOutputStreamOperator<StrategyDocument> strategyDocumentDataStreamSource = textStream.map((String value) -> {
            JSONObject jsonObject = JSON.parseObject(value);
            StrategyDocument strategyDocument = new StrategyDocument();
            strategyDocument.setId("200_" + jsonObject.getLong("id"));
            strategyDocument.setAddress(jsonObject.getString("address"));
            strategyDocument.setFees(new JSONObject());
            strategyDocument.getFees().put(jsonObject.getString("coin"), jsonObject.getBigDecimal("coinFee").toPlainString());
            strategyDocument.getFees().put(jsonObject.getString("goods"), jsonObject.getBigDecimal("goodsFee").toPlainString());
            strategyDocument.setAddressType(100);
            JSONObject endTime = jsonObject.getJSONObject("endTime");
            Calendar instance = Calendar.getInstance();
            if (endTime != null) {
                instance.set(endTime.getIntValue("year"), endTime.getIntValue("monthValue"), endTime.getIntValue("dayOfMonth"), endTime.getIntValue("hour"), endTime.getIntValue("minute"), endTime.getIntValue("second"));
                instance.set(Calendar.MILLISECOND, endTime.getIntValue("nano"));
                strategyDocument.setEndTime(instance.getTime());
            }
            strategyDocument.setStatus(jsonObject.getInteger("status"));
            strategyDocument.setAviCoinAmount(jsonObject.getBigDecimal("profitCoin").subtract(jsonObject.getBigDecimal("extractedCoin")).toPlainString());
            strategyDocument.setUnsoldCoinAmount(jsonObject.getBigDecimal("coinAmount").subtract(jsonObject.getBigDecimal("profitCoin")).toPlainString());
            BigDecimal cumulativeUsageAmount = jsonObject.getBigDecimal("oriAmount").subtract(jsonObject.getBigDecimal("coinAmount").subtract(jsonObject.getBigDecimal("profitCoin")));
            strategyDocument.setCumulativeUsageAmount(cumulativeUsageAmount.toPlainString());
            JSONObject createdAt = jsonObject.getJSONObject("createdAt");
            instance.set(createdAt.getIntValue("year"), createdAt.getIntValue("monthValue"), createdAt.getIntValue("dayOfMonth"), createdAt.getIntValue("hour"), createdAt.getIntValue("minute"), createdAt.getIntValue("second"));
            instance.set(Calendar.MILLISECOND, createdAt.getIntValue("nano"));
            strategyDocument.setStartTime(instance.getTime());
            strategyDocument.setOriCoinAmount(jsonObject.getBigDecimal("oriAmount").toPlainString());
            strategyDocument.setType(200);
            return strategyDocument;
        }).returns(TypeInformation.of(StrategyDocument.class)).name("映射网格");
        SinkFunction sink = new MongodbUpsertSink("127.0.0.1",27017,"bi","strategy");
        strategyDocumentDataStreamSource.addSink(sink);
        // 触发任务执行
        streamExecutionEnvironment.execute("gridJob");
    }
}
