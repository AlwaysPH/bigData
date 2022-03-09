package com.flink.java;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * flink自定义分区
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class PartitionJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> text = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        text.map(e -> e).setParallelism(2).partitionCustom(new MyPartitioner(), s -> s).print().setParallelism(4);

        env.execute("PartitionJava");
    }

    public static class MyPartitioner implements Partitioner<Integer>{

        @Override
        public int partition(Integer integer, int i) {
            if(integer % 2 == 0){
                return 0;
            }else {
                return 1;
            }
        }
    }
}
