package com.flink.java.state;

import com.flink.java.model.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyedState 键控状态
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("bigData04", 7777);

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        SingleOutputStreamOperator<Integer> result = dataStream.keyBy(e -> e.getId())
                .map(new MyKeyedMapper());

        result.print();
        env.execute();
    }

    public static class MyKeyedMapper extends RichMapFunction<User, Integer>{

        //ValueState
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //ValueState初始化
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));
        }

        @Override
        public Integer map(User value) throws Exception {
            Integer count = valueState.value() == null ? 0 : valueState.value();
            count++;
            valueState.update(count);
            return count;
        }
    }

}
