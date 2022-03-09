package com.flink.java.window;

import com.flink.java.model.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * window API 求平均数
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class WindowCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("bigData04", 7777);

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        SingleOutputStreamOperator<Double> result = dataStream.keyBy(s -> s.getId())
                .countWindow(10, 2)
                .aggregate(new AverageFunction());
        result.print();
        env.execute();
    }

    /**
     * Tuple2<Integer, Integer> 保存中间累加结果和个数
     */
    public static class AverageFunction implements AggregateFunction<User, Tuple2<Integer, Integer>, Double>{

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(User value, Tuple2<Integer, Integer> accumulator) {
            return new Tuple2<>(value.getAge() + accumulator.f0, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return Double.valueOf(String.valueOf(accumulator.f0 / accumulator.f1));
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
