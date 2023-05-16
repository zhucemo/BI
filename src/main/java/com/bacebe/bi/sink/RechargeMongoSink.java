package com.bacebe.bi.sink;


import com.alibaba.fastjson2.JSON;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;


public class RechargeMongoSink extends RichSinkFunction<String> {


    private transient MongoClient mongoClient;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ConfigOption<String> option = ConfigOptions.key("host").stringType().defaultValue("127.0.0.1");
        String host = parameters.get(option);

        ConfigOption<String> t = ConfigOptions.key("t").stringType().noDefaultValue();
        String tt = parameters.get(t);
        System.out.println(tt);

        ConfigOption<Integer> portOption = ConfigOptions.key("port").intType().defaultValue(27017);
        Integer port = parameters.get(portOption);

        mongoClient = new MongoClient(host, port);
    }

    @Override
    public void invoke(String bean, Context context) throws Exception {
        try {

            MongoDatabase db = mongoClient.getDatabase("bi");
            MongoCollection<Document> t = db.getCollection("recharge");
            Document document = new Document();
            document.putAll(JSON.parseObject(bean));
            t.insertOne(document);

        } catch (Exception e) {
            if (null != mongoClient) {
                mongoClient.close();
                mongoClient = null;
                System.out.println("里面关闭啦！");
            }
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (null != mongoClient) {
                mongoClient.close();
                mongoClient = null;
                System.out.println("关闭啦！");
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
    }
}