 package com.bigdata.flink.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile("hdfs://bigData01:9000/hello.txt");

        AggregateOperator<Tuple2<String, Integer>> count = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String i : s.split(" ")) {
                    collector.collect(new Tuple2<>(i, 1));
                }
            }
        }).groupBy(0).sum(1).setParallelism(1);

        count.writeAsCsv("hdfs://bigData01:9000/out");
        env.execute("BatchWordCount");
    }
}
