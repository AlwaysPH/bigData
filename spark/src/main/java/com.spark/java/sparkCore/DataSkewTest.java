package com.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

/**
 * spark数据倾斜处理：key随机前缀
 */
public class DataSkewTest {

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();
        JavaRDD<String> fileRDD = sc.textFile(args[0]);
        JavaRDD<String> mapRDD = fileRDD.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        JavaPairRDD<String, Long> tRDD = mapRDD.mapToPair(e -> new Tuple2<String, Long>(e, 1L));
        //第一步，给RDD中的每个key都打上一个随机前缀
        JavaPairRDD<String, Long> randomRDD = tRDD.mapToPair(e -> {
            Random random = new Random();
            int i = random.nextInt(10);
            return new Tuple2<String, Long>(i + "_" + e._1, e._2);
        });

        //第二步，对打上随机前缀的key进行局部聚合
        JavaPairRDD<String, Long> reduceRDD = randomRDD.reduceByKey((x, y) -> x + y);

        // 第三步，去除RDD中每个key的随机前缀
        JavaPairRDD<String, Long> rdd = reduceRDD.mapToPair(e -> {
            String original = e._1.split("_")[1];
            return new Tuple2<String, Long>(original, e._2);
        });

        // 第四步，对去除了随机前缀的RDD进行全局聚合
        JavaPairRDD<String, Long> result = rdd.reduceByKey((x, y) -> x + y);

//        result.foreach(e -> System.out.println(e._1 + "~" + e._2));

        result.saveAsTextFile(args[1]);

    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("DataSkewTest");
//                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
