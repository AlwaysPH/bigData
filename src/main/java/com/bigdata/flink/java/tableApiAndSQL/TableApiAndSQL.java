package com.bigdata.flink.java.tableApiAndSQL;

import com.bigdata.flink.java.model.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableApiAndSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, setting);

        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        //将DataStream转换成table
        Table user = tEnv.fromDataStream(dataStream);
        Table table = user.select($("id"), $("name"), $("age"), $("time"))
                .where($("age").isGreater(20));
        table.execute().print();

        //将DataStream注册成视图
        tEnv.createTemporaryView("t_dataStream_user", dataStream, $("id"), $("name"), $("age"), $("time"));

        //将Table注册成视图
        tEnv.createTemporaryView("t_user", user);
        String sql = "select id, name, age from t_user";
        Table resultTable = tEnv.sqlQuery(sql);

        //table转换为dataStream(flink 1.14版本可以直接使用tEnv.toDataStream(resultTable))
        DataStream<Row> data = tEnv.toAppendStream(resultTable, Row.class);
        data.print();
        env.execute();

    }
}
