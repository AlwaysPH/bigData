package com.bigdata.flink.java.transformation;

import com.bigdata.flink.java.model.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * DataStream分流和合流
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class MutlTransform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.readTextFile("data/ph.txt");
        SingleOutputStreamOperator<User> map = text.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        //分流
        SplitStream<User> splitStream = map.split(new OutputSelector<User>() {
            @Override
            public Iterable<String> select(User user) {
                return user.getAge() > 28 ? Collections.singletonList("old") : Collections.singletonList("young");
            }
        });

        DataStream<User> old = splitStream.select("old");
        DataStream<User> young = splitStream.select("young");

        old.print("old");
        young.print("young");

        //合流
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> tMap = old.map(new MapFunction<User, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(User user) throws Exception {
                return new Tuple3<Integer, String, Integer>(user.getId(), user.getName(), user.getAge());
            }
        });

        ConnectedStreams<Tuple3<Integer, String, Integer>, User> connect = tMap.connect(young);

        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<Tuple3<Integer, String, Integer>, User, Object>() {
            @Override
            public Object map1(Tuple3<Integer, String, Integer> value) throws Exception {
                return new Tuple4<Integer, String, Integer, String>(value.f0, value.f1, value.f2, "太老了");
            }

            @Override
            public Object map2(User value) throws Exception {
                return new Tuple4<Integer, String, Integer, String>(value.getId(), value.getName(), value.getAge(), "很年轻");
            }
        });
        result.print();

        env.execute("MutlTransform");
    }
}
