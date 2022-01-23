package com.bigdata.flink.java.tableApiAndSQL.window;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Table API和SQL定义Over Windows
 * over window 聚合为每个输入行在其相邻行的范围内计算聚合
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class OverWindow {

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

        /**table API**/
        //无界
        // 无界 over window 通过常量来指定，
        // 例如，用UNBOUNDED_RANGE指定时间间隔或用 UNBOUNDED_ROW 指定行计数间隔。
        // 无界 over windows 从分区的第一行开始
        Table unboundTableResult = table.window(Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_RANGE).as("ow"))
                .select($("id"), $("id").count().over($("ow")).as("idCount"),
                        $("age").avg().over($("ow")).as("ageAvg"), $("pt"));

        //有界
        //有界 over window 用间隔的大小指定
        // 时间间隔:lit(1).minutes()
        // 行计数间隔:rowInterval(2L)
        Table boundTableResult = table.window(Over.partitionBy($("id")).orderBy($("pt")).preceding(rowInterval(2L)).as("ow"))
                .select($("id"), $("id").count().over($("ow")).as("idCount"),
                        $("age").avg().over($("ow")).as("ageAvg"), $("pt"));

        /**SQL**/
        tableEnv.createTemporaryView("t_user", table);
        //RANGE 指定时间间隔
        //ROWS  指定行计数间隔
        Table sqlResult = tableEnv.sqlQuery("select id, count(id) over ow as idCount, avg(age) over ow as ageAvg, pt" +
                " from t_user window ow as (partition by id order by pt rows between 2 preceding and current row)");
        sqlResult.execute().print();

    }
}
