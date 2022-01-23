package com.bigdata.flink.java.tableApiAndSQL.window;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Table API和SQL定义Group Windows
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class GroupWindow {

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

        /**滚动窗口**/
        //table api
        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("name"), $("age"),
                $("time").as("logTime"), $("pt").rowtime());
        Table resultTable = table.window(Tumble.over(lit(5).seconds()).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("id").count().as("num"), $("age").avg().as("ageAvg"),
                        $("w").end().as("endTime"));

        //SQL
        tableEnv.createTemporaryView("t_user", table);
        Table sqlResult = tableEnv.sqlQuery("select id, count(id) as num, avg(age) as ageAvg, TUMBLE_START(pt, INTERVAL '5' second) as endTime" +
                " from t_user group by id, TUMBLE(pt, INTERVAL '5' second)");


        /**滑动窗口**/
        //table api
        Table sResultTable = table.window(Slide.over(lit(20).seconds()).every(lit(5).seconds()).on($("pt")).as("s"))
                .groupBy($("id"), $("s"))
                .select($("id"), $("id").count().as("num"), $("age").avg().as("ageAvg"), $("logTime").avg().as("logTimeAvg"), $("s").end().as("endTime"));

        //SQL
        Table sSqlResult = tableEnv.sqlQuery("select id, count(id) as num, avg(age) as ageAvg, avg(logTime) as logTimeAvg, HOP_END(pt, INTERVAL '5' second, INTERVAL '20' second) as endTime" +
                " from t_user group by id, HOP(pt, INTERVAL '5' second, INTERVAL '20' second)");

        sResultTable.execute().print();
        sSqlResult.execute().print();

    }
}
