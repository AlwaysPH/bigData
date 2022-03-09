package com.flink.java.tableApiAndSQL.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * Table API数据写入ES
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableApiToES {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /**从kafka读取数据**/
        tableEnv.executeSql("create table t_user(id int, name string, age int)" +
                "with (" +
                "'connector' = 'kafka-0.11'," +
                "'topic' = 'kafkaTest'," +
                "'properties.bootstrap.servers' = 'bigData04:9092'," +
                "'properties.group.id' = 'kafkaTest'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv')");
        String sql = "select id, name, age from t_user where age > 25";
        Table result = tableEnv.sqlQuery(sql);

        //从kafka读取的数据写入到es
        tableEnv.executeSql("create table es_user(id int, name string, age int, PRIMARY KEY (id) NOT ENFORCED)" +
                "with(" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://bigData04:9200'," +
                "'index' = 'userinfo'," +
                "'sink.bulk-flush.max-actions' = '1000')");
        result.executeInsert("es_user");
    }
}
