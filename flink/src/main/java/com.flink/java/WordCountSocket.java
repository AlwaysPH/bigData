package com.flink.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class WordCountSocket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("bigData04", 9001);

        SingleOutputStreamOperator<Tuple2<String, Integer>> count = text.flatMap(new Splitter())
            .keyBy(s -> s.f0).timeWindow(Time.seconds(2)).sum(1);

        count.print().setParallelism(1);
        env.execute("WordCountSocket");
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String i : words){
                collector.collect(new Tuple2<String, Integer>(i, 1));
            }
        }
    }
}
