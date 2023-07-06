package com.bacebe.bi.window;


import jdk.nashorn.internal.runtime.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

@Slf4j
public class ProfitWindow  implements WindowFunction<Tuple2<String, BigDecimal>, Tuple2<String, BigDecimal>, String, TimeWindow> {





    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, BigDecimal>> input, Collector<Tuple2<String, BigDecimal>> out) throws Exception {
        BigDecimal sum = BigDecimal.ZERO;
        for(Tuple2<String,BigDecimal> tuple2 : input){
            log.info("window profit----------> {}", tuple2);
            sum = sum.add(tuple2.f1);
        }
        log.info("profit ---> {}", sum.toPlainString());
        out.collect(new Tuple2<>(key, sum));
    }
}
