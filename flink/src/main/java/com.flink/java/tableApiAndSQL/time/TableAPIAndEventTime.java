package com.flink.java.tableApiAndSQL.time;

import com.flink.java.model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API事件时间特性(Event Time)
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableAPIAndEventTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);


        /**通过流式转换Table时，定义事件时间属性(Event Time)**/
        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((user, time) -> user.getTime()));

        //DataStream流转换成Table时，用 .rowtime() 后缀来定义事件时间属性(Event Time)
        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("name"), $("age"), $("time").as("logTime"), $("pt").rowtime());
        table.printSchema();

        /**创建DDL时，定义事件时间属性(Event Time)**/
        //声明 rt 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
        tableEnv.executeSql("create table t_user(id int, " +
                "name string, " +
                "age int, " +
                "logTime bigint, " +
                "rt AS TO_TIMESTAMP_LTZ(ts, 3)," + //字段类型需转换成为timestamp(3)类型
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND)" +
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
