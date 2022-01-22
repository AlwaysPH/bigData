package com.bigdata.flink.java.tableApiAndSQL.time;

import com.bigdata.flink.java.model.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API处理时间特性(Processing Time)
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableAPIAndProcessingTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        /**通过流式转换Table时，定义处理时间属性(Processing Time)**/
        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        //DataStream流转换成Table时，用 .proctime() 后缀来定义处理时间属性(Processing Time)
        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("name"), $("age"), $("time").as("logTime"), $("pt").proctime());
        table.printSchema();

        /**创建DDL时，定义处理时间属性(Processing Time)**/
        tableEnv.executeSql("create table t_user(id int, " +
                "name string, " +
                "age int, " +
                "logTime bigint, " +
                "ptTime as PROCTIME())" +
                "with (" +
                "'connector' = 'kafka-0.11'," +
                "'topic' = 'kafkaTest'," +
                "'properties.bootstrap.servers' = 'bigData04:9092'," +
                "'properties.group.id' = 'kafkaTest'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv')");


        env.execute();

    }
}
