package com.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("words");
//                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "data\\words.txt";
        if(args.length == 1){
            path = args[0];
        }
        JavaRDD<String> javaRDD = sc.textFile(path);
        JavaPairRDD<String, Integer> resultRDD = javaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((x, y) -> x + y);

        resultRDD.foreach(e -> System.out.println(e._1 + "~" + e._2));
    }
}