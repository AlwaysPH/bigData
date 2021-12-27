package com.bigdata.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkArrayTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkArrayTest")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        JavaRDD<Integer> rdd = sc.parallelize(list);

        int sum = rdd.reduce((x, y) -> x + y);

        System.out.println(sum);

        sc.stop();
    }
}
