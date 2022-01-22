package com.bigdata.flink.java.tableApiAndSQL.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Table API写入数据到JDBC
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableApiToJDBC {

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

        //数据写入到mysql
        tableEnv.executeSql("create table t_mysql_user(id int, name string, age int, primary key (id) NOT ENFORCED)" +
                "with(" +
                "'connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = 'jdbc:mysql://localhost:3306/cas?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false&AllowPublicKeyRetrieval=True'," +
                "'table-name' = 't_flink_user'," +
                "'username' = 'root'," +
                "'password' = '888888')");
        //overwrite为true，会覆盖掉id相同的数据
        //executeInsert只有一个参数时，默认会覆盖掉id相同的数据
        result.executeInsert("t_mysql_user", false);
    }
}
