package com.flink.java.window;

import com.flink.java.model.User;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * window API 全窗口函数
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class WindowAllFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStreamSource<String> source = env.socketTextStream("bigData04", 7777);

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        SingleOutputStreamOperator<Tuple3<Long, Long, Integer>> result = dataStream.keyBy(s -> s.getId())
                .timeWindow(Time.seconds(5))
                //ProcessWindowFunction全窗口函数
//                .process(new ProcessWindowFunction<User, Object, Integer, TimeWindow>() {
//                })
                //WindowFunction全窗口函数
                .apply(new MyWindowFunction());

        result.print();
        env.execute();
    }

    /**
     * 自定义全窗口函数
     */
    public static class MyWindowFunction implements WindowFunction<User, Tuple3<Long, Long, Integer>, Integer, TimeWindow> {

        Long sum = 0L;

        @Override
        public void apply(Integer key, TimeWindow window, Iterable<User> input, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            Integer count = IteratorUtils.toList(input.iterator()).size();
            //获取事件处理完成时间
            Long time = window.getEnd();
            for (User user : input) {
                sum += user.getAge();
            }
            out.collect(new Tuple3<Long, Long, Integer>(sum, time, key));
        }
    }
}
