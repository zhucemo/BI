package com.bacebe.bi.model;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;


import java.util.Date;

@Data
public class StrategyDocument {

    private String id;

    private Integer type;

    private Date startTime;

    private Date endTime;

    private String address;

    private Integer addressType;

    private Integer status;

    private String  oriCoinAmount;

    private String  aviCoinAmount;

    private String unsoldCoinAmount;

    private String cumulativeUsageAmount;

    private JSONObject fees;


}
