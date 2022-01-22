package com.bigdata.flink.java.watermarks;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * watermark代码实现
 * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class Watermarks {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);
        DataStream<User> dataStream = text.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        })
                //乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((user, timestamp) -> user.getTime()));
                //Time.seconds(2)设置2秒延迟时间
//        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<User>(Time.seconds(2)) {
//            @Override
//            public long extractTimestamp(User element) {
//                return element.getTime();
//            }
//        });

        //侧输出流标记
        OutputTag<User> outputTag = new OutputTag<User>("late"){};
        SingleOutputStreamOperator<User> minStream = dataStream.keyBy(e -> e.getId())
                .timeWindow(Time.seconds(15))
                //设置迟到数据延迟时间
                .allowedLateness(Time.minutes(1))
                //设置侧输出流
                .sideOutputLateData(outputTag)
                .minBy("age");

        minStream.print();
        //获取侧输出流数据
        minStream.getSideOutput(outputTag);
        env.execute();

    }

    /**
     * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
     * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
     */
    public static class MyWaterMarks implements WatermarkGenerator{

        private final long maxTimeLag = 3500;

        private long currentMaxTimestamp;

        @Override
        public void onEvent(Object o, long eventTime, WatermarkOutput watermarkOutput) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTime);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxTimeLag - 1));
        }
    }
}
