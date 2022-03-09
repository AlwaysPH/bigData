package com.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class ShareValueTest {

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        LongAccumulator acc = sc.sc().longAccumulator();
        rdd.foreach(e -> acc.add(e));
        System.out.println(acc);
    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkArrayTest")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}


