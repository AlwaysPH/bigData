package com.bigdata.flink.java.proccessAPI;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class KeyedProFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);

        SingleOutputStreamOperator<User> data = text.map(e -> {
            String[] str = e.split(",");
            return new User(Integer.valueOf(str[0]), str[1], Integer.valueOf(str[2]), Long.valueOf(str[3]));
        });

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result = data.keyBy(s -> s.getId())
                .process(new MyKeyedProcessFunction());
        result.print();

        env.execute();
    }

    //统计key的总数
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Integer, User, Tuple2<Integer, Integer>> {

        private ValueState<CountWithTimestamp> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("count", CountWithTimestamp.class));
        }

        @Override
        public void processElement(User value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            //当前key值
            Integer currentKey = ctx.getCurrentKey();
            CountWithTimestamp data = valueState.value();
            if(null == data){
                data = new CountWithTimestamp();
                data.key = currentKey;
            }

            data.count++;
            //当前数据事件
            data.lastModified = ctx.timestamp() == null ? 0L : ctx.timestamp();

            valueState.update(data);

            //可以获取侧输出流
//            ctx.output();

            //获取当前处理时间
            ctx.timerService().currentProcessingTime();
            //获取当前事件事件
            ctx.timerService().currentWatermark();

            //设置事件时间定时器(从当前事件时间开始 60 秒设置一个定时器)
//            ctx.timerService().registerEventTimeTimer(data.lastModified + 60000L);

            //设置处理时间定时器(从当前处理时间开始 60 秒设置一个定时器)
            ctx.timerService().registerProcessingTimeTimer(data.lastModified + 60000L);
        }

        @Override
        public void close() throws Exception {
            valueState.clear();
        }

        //定时器执行操作，当前timestamp为定时器时间
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            CountWithTimestamp result = valueState.value();

            if (timestamp == result.lastModified + 60000) {
                out.collect(new Tuple2<Integer, Integer>(result.key, result.count));
            }
        }
    }

    public static class CountWithTimestamp{
        private Integer key;

        private Integer count = 0;

        private Long lastModified = 0L;
    }
}
