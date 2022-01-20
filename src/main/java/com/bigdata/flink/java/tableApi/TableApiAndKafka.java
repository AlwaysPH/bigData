package com.bigdata.flink.java.tableApi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Table API读取kafka和数据写入kafka
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableApiAndKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /**从kafka读取数据**/
        tableEnv.executeSql("create table t_user(id int, name string, age int)" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'kafkaTest'," +
                "'properties.bootstrap.servers' = 'bigData04:9092'," +
                "'properties.group.id' = 'test'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv')");
        //table转换成DataStream
//        Table user = tableEnv.from("t_user");
//        DataStream<Row> dataStream = tableEnv.toAppendStream(user, Row.class);
//        dataStream.print();

        String sql = "select id, name, age from t_user where age > 26";
        Table result = tableEnv.sqlQuery(sql);

        /**数据写入到kafka**/
        tableEnv.executeSql("create table out_user(id int, name string, age int)" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'outTest'," +
                "'properties.bootstrap.servers' = 'bigData04:9092'," +
                "'properties.group.id' = 'test'," +
                "'format' = 'json')");
        result.executeInsert("out_user");
        env.execute();

    }
}
