package com.flink.java.state;

import com.flink.java.model.User;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink 状态编程，状态表示算子中间值（flink容灾）
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class ExampleKeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);
        SingleOutputStreamOperator<User> data = text.map(e -> {
            String[] s = e.split(",");
            return new User(Integer.valueOf(s[0]), s[1], Integer.valueOf(s[2]), Long.valueOf(s[3]));
        });

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> result = data.keyBy(s -> s.getId())
                .flatMap(new AgeWarning(5));
        result.print();
        env.execute();
    }

    public static class AgeWarning extends RichFlatMapFunction<User, Tuple3<Integer, Integer, Integer>> {

        private Integer flag;

        public AgeWarning(Integer flag) {
            this.flag = flag;
        }

        ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("old", Integer.class));
        }

        @Override
        public void flatMap(User user, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
            Integer data = valueState.value();
            if(null != data){
                Integer i = Math.abs(user.getAge() - data);
                if(i >= flag){
                    out.collect(new Tuple3<Integer, Integer, Integer>(user.getId(), user.getAge(), data));
                }
            }
            //更新状态
            valueState.update(user.getAge());
        }

        @Override
        public void close() throws Exception {
            valueState.clear();
        }
    }
}
