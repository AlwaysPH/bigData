package com.bigdata.flink.java.window;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * window API
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class WindowType {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> source = env.readTextFile("data/ph.txt");
        DataStreamSource<String> source = env.socketTextStream("bigData04", 7777);

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        dataStream.keyBy(s -> s.getId())
                //会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(2)));
                //滑动计数窗口
//                .countWindow(50, 5);
                //滚动计数窗口
//                .countWindow(20);
                //滑动时间窗口
//                .timeWindow(Time.seconds(30), Time.seconds(5));
                //滚动时间窗口
                .timeWindow(Time.seconds(5));

        env.execute();
    }
}
