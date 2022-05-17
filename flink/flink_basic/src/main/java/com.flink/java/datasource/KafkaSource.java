package com.flink.java.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * flink连接kafka获取数据源
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class KafkaSource {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigData04:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("topic", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);

        //todo 数据处理

    }
}
