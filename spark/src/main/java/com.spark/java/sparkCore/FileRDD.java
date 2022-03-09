package com.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FileRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkArrayTest")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile("data\\words.txt");

        JavaRDD<Integer> map = file.map(e -> e.length());

        Integer res = map.reduce((x, y) -> x + y);

        System.out.println(res);
    }
}
