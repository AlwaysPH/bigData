package com.bigdata.spark.java.sparkCore;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MapPartitionTest {

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        Integer sum = rdd.mapPartitions(e -> {
            List<Integer> list = Lists.newArrayList();
            while (e.hasNext()) {
                list.add(e.next());
            }
            return list.iterator();
        }).reduce((x, y) -> x + y);
        System.out.println(sum);
    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("MapPartitionTest")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
