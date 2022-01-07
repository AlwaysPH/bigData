package com.bigdata.flink.java.sink;

import com.bigdata.flink.java.model.User;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * flink连接ES
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class StreamToEsSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/ph.txt");

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]));
        });

        //指定es地址（含es集群）
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("bigData04", 9200, "http"));

        ElasticsearchSink<User> esBuild = new ElasticsearchSink.Builder<User>(httpHosts, new MyESFunction()).build();

        dataStream.addSink(esBuild);
        env.execute();
    }

    //自定义ES写入操作类
    public static class MyESFunction implements ElasticsearchSinkFunction<User>{

        @Override
        public void process(User user, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            requestIndexer.add(getRequestData(user));
        }

        public IndexRequest getRequestData(User user){
            //定义写入数据
            HashMap<String, Object> dataSource = Maps.newHashMap();
            dataSource.put("id", user.getId());
            dataSource.put("name", user.getName());
            dataSource.put("age", user.getAge());
            dataSource.put("time", System.currentTimeMillis());

            //创建请求，向ES发送写入命令
            return Requests.indexRequest().index("userinfo").type("data").source(dataSource);
        }
    }
}
