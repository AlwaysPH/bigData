package com.bigdata.flink.java.tableApiAndSQL;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class DataStreamToTable {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple2<Integer, String>> list = Lists.newArrayList();
        list.add(new Tuple2<Integer, String>(1, "ph"));
        list.add(new Tuple2<Integer, String>(2, "ypy"));
        list.add(new Tuple2<Integer, String>(3, "haha"));

        DataStreamSource<Tuple2<Integer, String>> dataStreamSource = env.fromCollection(list);

        //将DataStream转换成table
        Table table = tEnv.fromDataStream(dataStreamSource, $("id"), $("name"));
        table.select($("id"), $("name")).filter($("id").isLess(3)).execute().print();

        //将DataStream转换成视图
        tEnv.createTemporaryView("t", dataStreamSource, $("id"), $("name"));
        tEnv.sqlQuery("select id, name from t where id < 3").execute().print();

    }
}
