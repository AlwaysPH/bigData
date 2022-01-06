package com.bigdata.flink.java.sink;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * 数据写入kafka
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class StreamToKafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/ph.txt");

        DataStream<String> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2])).toString();
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigData04:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        dataStream.addSink(new FlinkKafkaProducer011<String>("kafkaTest", new SimpleStringSchema(), properties));

        //从kafka读取数据
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("kafkaTest", new SimpleStringSchema(), properties);
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        dataStreamSource.print();

        env.execute();
    }

}
