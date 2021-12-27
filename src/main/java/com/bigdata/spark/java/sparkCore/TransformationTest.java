package com.bigdata.spark.java.sparkCore;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class TransformationTest {

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();
        //map
//        mapOp(sc);

        //filter
//        filterOp(sc);

        //groupByKey
//        groupByKeyOp(sc);

        //reduceByKey
        reduceOp(sc);

        sc.stop();
    }

    private static void reduceOp(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> list = Lists.newArrayList(
                new Tuple2<>("力量", 12),
                new Tuple2<>("敏捷", 11),
                new Tuple2<>("敏捷", 5),
                new Tuple2<>("智力", 6),
                new Tuple2<>("智力", 9)
        );

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        rdd.reduceByKey((x, y) -> x + y).mapToPair(i -> new Tuple2<>(i._2, i._1)).sortByKey(false).foreach(e -> System.out.println(e._1 + "~" + e._2));

    }

    private static void groupByKeyOp(JavaSparkContext sc) {
        List<Tuple2<String, String>> list = Lists.newArrayList(
                new Tuple2<>("力量", "撼地神牛"),
                new Tuple2<>("敏捷", "影魔"),
                new Tuple2<>("敏捷", "幻影刺客"),
                new Tuple2<>("智力", "卡尔"),
                new Tuple2<>("智力", "冰女妹妹")
        );

        JavaPairRDD<String, String> rdd = sc.parallelizePairs(list);
        rdd.groupByKey().foreach(e -> {
            String type = e._1;
            Iterator<String> iterator = e._2.iterator();
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()){
                sb.append(",").append(iterator.next());
            }
            if (sb.length() > 0){
                sb.deleteCharAt(0);
            }
            System.out.println("类型：" + type + " 模型：" + sb.toString());
        });

    }

    private static void filterOp(JavaSparkContext sc) {
        List<Integer> list = Lists.newArrayList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.filter(e -> e % 2 == 0).foreach(i -> System.out.println(i));
    }

    private static void mapOp(JavaSparkContext sc) {
        List<Integer> list = Lists.newArrayList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        rdd.map(e -> e * 2).foreach(i -> System.out.println(i));
    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkArrayTest")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
