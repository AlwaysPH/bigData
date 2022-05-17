package com.flink.java.tableApiAndSQL.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * table api 连接外部系统读取数据
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class ReadFromOtherSystem {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        String path = "E:\\workspace\\bigData\\data\\ph.txt";
        String outPath = "E:\\workspace\\bigData\\data\\out.txt";

        //connect实现(已过时)
//        tableEnv.connect(new FileSystem().path(path))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.INT())
//                        .field("name", DataTypes.STRING())
//                        .field("age", DataTypes.INT())
//                        .field("time", DataTypes.BIGINT())
//                )
//                .createTemporaryTable("t_user");
//
//        Table tUser = tableEnv.from("t_user");
//        tUser.select($("id"), $("name"), $("age"), $("time"))
//                .where($("age").isGreater(25)).execute().print();

        tableEnv.executeSql("create table t_user (id int, name string, age int, logTime BIGINT)" +
                " with ('connector.type' = 'filesystem'," +
                "'connector.path' = '"+path+"'," +
                "'format.type' = 'csv')");
//        Table user = tableEnv.from("t_user");
//        user.select($("id"), $("name"), $("age"), $("logTime"))
//                .where($("age").isGreater(25)).execute().print();

        String sql = "select id, name, age, logTime from t_user where age > 26";
        tableEnv.executeSql(sql).print();

        /**查询结果输出到文件**/
        //注册外部输出表
        tableEnv.executeSql("create table output (id int, name string, age int, logTime BIGINT)" +
                " with ('connector.type' = 'filesystem'," +
                "'connector.path' = '"+outPath+"'," +
                "'format.type' = 'csv')");
        Table result = tableEnv.sqlQuery(sql);
        result.executeInsert("output");


    }
}
