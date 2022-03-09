package com.flink.java.tableApiAndSQL.udf;

import com.flink.java.model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * UDF自定义函数
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class UdfExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((user, timestamp) -> user.getTime()));

        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("name"), $("age"),
                $("time").as("logTime"), $("pt").rowtime());

        //UDF函数注册
        init(tableEnv);
        tableEnv.createTemporaryView("t_user", table);

        /**标量函数**/
        //table api实现
        Table tableResult = table.select($("id"), call("NOT_NULL", $("name")).as("flag"), $("pt"));
        //sql实现
        Table sqlResult = tableEnv.sqlQuery("select id, NOT_NULL(name) as flag, pt from t_user");

        /**表函数**/
        //table api实现
        Table result = table.joinLateral(call("SPLIT", $("name"), "_").as("newName", "length"))
                .select($("id"), $("name"), $("newName"), $("length"));
        //sql实现
        Table sqlTableResult = tableEnv.sqlQuery("select id, name, newName, length from t_user, LATERAL TABLE(SPLIT(name, '_')) as splitName(newName, length)");

        /**表函数**/
        //table api实现
        Table aggResult = table.groupBy($("id")).aggregate("AVG_AGG(age) as avg_age")
                .select($("id"), $("avg_age"));

        //sql实现
//        Table aggSqlResult = tableEnv.sqlQuery("select id, AVG_AGG(age) as ageTemp from t_user group by id");

        aggResult.execute().print();
    }

    private static void init(StreamTableEnvironment tableEnv) {
        FunctionLoader.INSTANCE.discoverFunctions().forEach(function -> {
            if (function instanceof ScalarFunction) {
                tableEnv.createTemporarySystemFunction(function.getName(), (ScalarFunction) function);
            } else if (function instanceof AggregateFunction) {
                tableEnv.createTemporarySystemFunction(function.getName(), (AggregateFunction<?, ?>) function);
            } else if (function instanceof TableFunction) {
                tableEnv.createTemporarySystemFunction(function.getName(), (TableFunction<?>) function);
            } else {
                throw new RuntimeException("Unsupported function type: " + function.getClass().getName());
            }
        });
    }

}
