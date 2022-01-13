package com.bigdata.flink.java.processAPI;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction、TimerService和定时器例子
 * 连续年龄上升，预警
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class ProcessExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);

        SingleOutputStreamOperator<User> data = text.map(e -> {
            String[] str = e.split(",");
            return new User(Integer.valueOf(str[0]), str[1], Integer.valueOf(str[2]), Long.valueOf(str[3]));
        });

        data.keyBy(s -> s.getId())
                .process(new AgeFunction(10)).print();
        env.execute();
    }

    //连续年龄上升，预警
    public static class AgeFunction extends KeyedProcessFunction<Integer, User, Tuple2<Integer, String>>{

        private Integer flag;

        public AgeFunction(Integer flag) {
            this.flag = flag;
        }

        private ValueState<Integer> lastAgeState;

        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastAgeState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last_age", Integer.class, 0));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time_data", Long.class));
        }

        @Override
        public void processElement(User user, Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
            Integer lastAge = lastAgeState.value();
            Long timer = timerState.value();

            //如果当前年龄上升并且没有定时器，注册定时器
            if(user.getAge() > lastAge && timer == null){
                Long ts = ctx.timerService().currentProcessingTime() + flag * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                //更新timerState状态
                timerState.update(ts);
            }else if(user.getAge() < lastAge){
                //如果年龄没有连续上升，则删除定时器
                ctx.timerService().deleteProcessingTimeTimer(timer);
                timerState.clear();
            }

            //更新lastAgeState状态
            lastAgeState.update(user.getAge());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
            //定时器触发，输出报警信息
            out.collect(new Tuple2<>(ctx.getCurrentKey(), "年龄太大了"));
            timerState.clear();
        }

        @Override
        public void close() throws Exception {
            lastAgeState.clear();
        }
    }
}
