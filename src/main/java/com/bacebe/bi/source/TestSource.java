package com.bacebe.bi.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class TestSource implements SourceFunction<String> {
    private Logger logger = LoggerFactory.getLogger(TestSource.class);


    public void run(SourceContext<String> ctx) throws Exception {
        while (true) {
            Thread.sleep(5000);
            ctx.collect("{\"id\":1674608898074177538,\"address\":\"0x2c7dbc0c187600efac5e2e024fc7f31d2981ae79\",\"coin\":\"USDT\",\"goods\":\"BETH\",\"priceLine\":[\"1894.900000\",\"1889.800000\",\"1884.700000\",\"1879.600000\",\"1874.500000\",\"1869.400000\",\"1864.300000\",\"1859.200000\",\"1854.100000\",\"1849\"],\"highestPrice\":1.9E+3,\"lowestPrice\":1849,\"type\":200,\"gap\":null,\"gridNum\":10,\"lowSingleProfitRate\":0,\"higSingleProfitRate\":0,\"oriAmount\":1E+2,\"feeRate\":0.0003,\"coinAmount\":2E+1,\"coinFee\":0,\"goodsAmount\":0.042934739087457856,\"goodsFee\":0.00001288428701234,\"profitCoin\":0,\"extractedCoin\":0,\"arbitrageTimes\":0,\"triggerPrice\":null,\"profitLimitPrice\":0,\"lossLimitPrice\":0,\"settlementType\":null,\"settlementCoin\":null,\"settlementGoods\":null,\"settlementEntrust\":null,\"surplusCoin\":null,\"surplusGoods\":null,\"status\":300,\"slippage\":0,\"endTime\":null,\"createdAt\":{\"dayOfYear\":181,\"month\":\"JUNE\",\"dayOfWeek\":\"FRIDAY\",\"nano\":0,\"year\":2023,\"monthValue\":6,\"dayOfMonth\":30,\"hour\":2,\"minute\":40,\"second\":48,\"chronology\":{\"calendarType\":\"iso8601\",\"id\":\"ISO\"}},\"updatedAt\":{\"dayOfYear\":181,\"month\":\"JUNE\",\"dayOfWeek\":\"FRIDAY\",\"nano\":0,\"year\":2023,\"monthValue\":6,\"dayOfMonth\":30,\"hour\":2,\"minute\":43,\"second\":16,\"chronology\":{\"calendarType\":\"iso8601\",\"id\":\"ISO\"}}}");
        }

    }

    @Override
    public void cancel() {

    }


}