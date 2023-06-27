package com.bacebe.bi.sink;


import com.alibaba.fastjson2.JSON;
import com.bacebe.bi.model.StrategyDocument;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;


@Slf4j
public class MongodbUpsertSink extends RichSinkFunction<StrategyDocument> {


    private transient MongoClient mongoClient;

    private String host,database,collection;
    private int port;

    public MongodbUpsertSink(String host, int port, String database, String collection){
        this.host=host;
        this.database=database;
        this.collection=collection;
        this.port=port;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        openDB();
    }

    @Override
    public void invoke(StrategyDocument bean, Context context) throws Exception {
        try {
            log.info("sink bean:{}",bean);
            if(mongoClient==null){
                openDB();
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> t = db.getCollection(collection);
            Document document = new Document();
            document.putAll(JSON.parseObject(JSON.toJSONBytes(bean)));
            Document filter = new Document();
            filter.put("id", document.get("id"));
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.upsert(true);
            t.updateOne(filter, document, updateOptions);

        } catch (Exception e) {
            if (null != mongoClient) {
                mongoClient.close();
                mongoClient = null;
                System.out.println("里面关闭啦！");
            }
            e.printStackTrace();
        }
    }

    private void openDB(){
        mongoClient = new MongoClient(host, port);
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