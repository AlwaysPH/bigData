package com.bigdata.spark.java.sparkCore;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class ActionTest {
    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();

//        countByKeyOp(sc);

        saveToFile(sc);
    }

    private static void saveToFile(JavaSparkContext sc) {
        List<Integer> list = Lists.newArrayList(1,2,3,4,5,6,7,8,9);

        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.saveAsTextFile("hdfs://bigData01:9000/data/out");
    }

    public static void countByKeyOp(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> list = Lists.newArrayList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 1),
                new Tuple2<>("b", 4),
                new Tuple2<>("c", 1),
                new Tuple2<>("d", 5),
                new Tuple2<>("e", 1),
                new Tuple2<>("e", 2)
        );
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        Map<String, Long> map = rdd.countByKey();
        for(Map.Entry<String, Long> entry : map.entrySet()){
            System.out.println(entry.getKey() + "~" + entry.getValue());
        }
    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkArrayTest")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
