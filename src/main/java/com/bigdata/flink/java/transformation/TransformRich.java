package com.bigdata.flink.java.transformation;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数操作
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TransformRich {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.readTextFile("data/ph.txt");
        SingleOutputStreamOperator<User> map = text.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = map.map(new MyMap());
        result.print();

        env.execute();
    }

    //获取此并行子任务的编号
    public static class MyMap extends RichMapFunction<User, Tuple2<String, Integer>>{

        @Override
        public Tuple2<String, Integer> map(User user) throws Exception {
            return new Tuple2<String, Integer>(user.getName(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //可以进行初始化，定义状态和数据库链接等
        }

        @Override
        public void close() throws Exception {
            //关闭工作，关闭数据库链接等
        }
    }
}
