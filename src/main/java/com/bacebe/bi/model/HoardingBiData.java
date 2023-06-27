package com.bacebe.bi.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class HoardingBiData {
    private String id;
    private String goodsO;
    private String goodsT;
    private BigDecimal goodsOAmount;
    private BigDecimal goodsTAmount;
    private Long createdTime;
    private Long endTime;
    private String address;
    private Integer addressType;
    private Integer status;
    private BigDecimal amount;
    private BigDecimal remainAmount;
    private BigDecimal notTradedAmount;
    private BigDecimal tradedAmount;
    private BigDecimal profitAmount;
    private BigDecimal fee;
    private String feeCoin;
    private Integer tradeStatus=0;
    private BigDecimal goodsTFee;
    private BigDecimal goodsOFee;
    private BigDecimal usdtFee;
    private BigDecimal goodsOTradedAmount;
    private BigDecimal goodsTTradedAmount;
    private BigDecimal totalTradedAmount;
}
