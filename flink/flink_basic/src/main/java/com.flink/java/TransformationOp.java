package com.flink.java;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TransformationOp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> list1 = Lists.newArrayList();
        list1.add(new Tuple2<>(1, "影魔"));
        list1.add(new Tuple2<>(2, "火猫"));
        list1.add(new Tuple2<>(3, "剑圣"));
        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(list1);

        List<Tuple2<Integer, String>> list2 = Lists.newArrayList();
        list2.add(new Tuple2<>(1, "跳刀"));
        list2.add(new Tuple2<>(2, "推推"));
        list2.add(new Tuple2<>(4, "隐刀"));
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(list2);

        text1.join(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                    }
                }).print();
    }
}
