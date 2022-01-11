package com.bigdata.flink.java.window;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * window API 增量聚合函数
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class WindowAddFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStreamSource<String> source = env.socketTextStream("bigData04", 7777);

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        SingleOutputStreamOperator<Integer> result = dataStream.keyBy(s -> s.getId())
                //会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(2)));
                //滑动计数窗口
//                .countWindow(50, 5);
                //滚动计数窗口
//                .countWindow(20);
                //滑动时间窗口
//                .timeWindow(Time.seconds(30), Time.seconds(5));
                //滚动时间窗口
                .timeWindow(Time.seconds(5))
                //ReduceFunction函数
//                .reduce(new MyReduceFunction());
                //AggregateFunction函数
                .aggregate(new MyAggregateFunction());

        result.print();
        env.execute();
    }

    /**
     * ReduceFunction函数
     */
    public static class MyReduceFunction implements ReduceFunction<User> {

        @Override
        public User reduce(User value1, User value2) throws Exception {
            value1.setAge(value1.getAge() + value2.getAge());
            return value1;
        }
    }

    /**
     * AggregateFunction函数
     */
    public static class MyAggregateFunction implements AggregateFunction<User, Integer, Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(User value, Integer accumulator) {
            return value.getAge() + accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }
}
