package com.flink.java.processAPI;

import com.flink.java.model.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ProcessFunction中侧输出流
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class SideOutputExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);

        SingleOutputStreamOperator<User> data = text.map(e -> {
            String[] str = e.split(",");
            return new User(Integer.valueOf(str[0]), str[1], Integer.valueOf(str[2]), Long.valueOf(str[3]));
        });

        OutputTag<User> outputTag = new OutputTag<User>("young"){};

        SingleOutputStreamOperator<User> youngStream = data.process(new ProcessFunction<User, User>() {
            @Override
            public void processElement(User user, Context ctx, Collector<User> out) throws Exception {
                if (user.getAge() > 28) {
                    out.collect(user);
                } else {
                    ctx.output(outputTag, user);
                }
            }
        });
        youngStream.print("年长的");
        youngStream.getSideOutput(outputTag).print("年轻的");
        env.execute();
    }
}
