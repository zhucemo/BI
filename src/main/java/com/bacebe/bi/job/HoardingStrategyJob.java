package com.bacebe.bi.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.bacebe.bi.model.HoardingBiData;
import com.bacebe.bi.model.StrategyDocument;
import com.bacebe.bi.sink.MongodbSink;
import com.bacebe.bi.sink.MongodbUpsertSink;
import com.bacebe.bi.source.RocketSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;


public class HoardingStrategyJob {



    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        // 获取socket输入数据
        RocketSource rocketSource=new RocketSource("127.0.0.1",9876,"BI-HOARDING","BI-HOARDING");
        DataStreamSource<String> textStream = streamExecutionEnvironment.addSource(rocketSource);
        SingleOutputStreamOperator<StrategyDocument> strategyDocumentDataStreamSource = textStream.map((String value) -> {
            HoardingBiData hoardingBiData = JSON.parseObject(value, HoardingBiData.class);
            StrategyDocument strategyDocument = new StrategyDocument();
            strategyDocument.setId("100_" + hoardingBiData.getId());
            strategyDocument.setType(100);
            strategyDocument.setAddressType(100);
            strategyDocument.setFees(new JSONObject());
            //TODO 映射字段
            strategyDocument.getFees().put(hoardingBiData.getGoodsO(), hoardingBiData.getGoodsOFee().toPlainString());
            strategyDocument.getFees().put(hoardingBiData.getGoodsT(), hoardingBiData.getGoodsTFee().toPlainString());
            strategyDocument.getFees().put("USDT", hoardingBiData.getUsdtFee().toPlainString());
            strategyDocument.setAddress(hoardingBiData.getAddress());
            strategyDocument.setAviCoinAmount(hoardingBiData.getRemainAmount().stripTrailingZeros().toPlainString());
            if(hoardingBiData.getEndTime()!=null) {
                strategyDocument.setEndTime(new Date(hoardingBiData.getEndTime()));
            }
            strategyDocument.setCumulativeUsageAmount(hoardingBiData.getProfitAmount().stripTrailingZeros().toPlainString());
            strategyDocument.setUnsoldCoinAmount(hoardingBiData.getNotTradedAmount().stripTrailingZeros().toPlainString());
            strategyDocument.setStartTime(new Date(hoardingBiData.getCreatedTime()));
            strategyDocument.setStatus(hoardingBiData.getStatus());
            strategyDocument.setOriCoinAmount(hoardingBiData.getAmount().stripTrailingZeros().toPlainString());
            return strategyDocument;
        }).returns(TypeInformation.of(StrategyDocument.class)).name("映射囤币");;
        SinkFunction sink = new MongodbUpsertSink("127.0.0.1",27017,"bi","strategy");
        strategyDocumentDataStreamSource.addSink(sink);
        // 触发任务执行
        streamExecutionEnvironment.execute("hoardingJob");
    }
}
