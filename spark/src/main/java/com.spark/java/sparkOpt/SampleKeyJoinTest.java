package com.spark.java.sparkOpt;

import com.spark.java.sparkSql.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

/**
 * 采样倾斜key并分拆join操作
 * 对于join导致的数据倾斜，如果只是某几个key导致了倾斜，采用该方式可以用最有效的方式打散key进行join。
 * 而且只需要针对少数倾斜key对应的数据进行扩容n倍，不需要对全量数据进行扩容。避免了占用过多内存
 */
public class SampleKeyJoinTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtil.getInstance().getSparkSession("SampleKeyJoinTest", "local[*]");

        //通过sparkSession获取JavaSparkContext
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> dataRDD = sc.textFile("data\\words.txt");

        JavaRDD<String> map = dataRDD.flatMap(e -> Arrays.asList(e.split(" ")).iterator());

        JavaPairRDD<String, Long> rdd = map.mapToPair(e -> new Tuple2<String, Long>(e, 1L));

        //首先从包含了少数几个导致数据倾斜key的rdd中，采样10%的样本数据
        JavaPairRDD<String, Long> sampleRDD = rdd.sample(false, 0.1);

        //通过降序排序，取出采样数据中key最多的前n个数据,并获取最多的key的值
        JavaPairRDD<String, Long> sMapRdd = sampleRDD.mapToPair(e -> new Tuple2<String, Long>(e._1, 1L));
        JavaPairRDD<String, Long> reduceRDD = sMapRdd.reduceByKey((x, y) -> x + y);
        JavaPairRDD<String, Long> revers = reduceRDD.mapToPair(e -> new Tuple2<>(e._1, e._2));
        Tuple2<String, Long> tuData = revers.sortByKey(false).take(1).get(0);
        String index = tuData._1;

        //从rdd中拆分出导致数据倾斜最多的key，单独为一个rdd，其他的单独为一个rdd
        JavaPairRDD<String, Long> skewedRDD = rdd.filter(e -> e._1.equals(index));
        JavaPairRDD<String, Long> commonRDD = rdd.filter(e -> !e._1.equals(index));

        //数据倾斜最多的skewedRDD，随机打散
        JavaPairRDD<String, Long> randomRDD = skewedRDD.mapToPair(e -> {
            Random random = new Random();
            int prefix = random.nextInt(100);
            return new Tuple2<String, Long>(prefix + "_" + e._1, e._2);
        });
        //打散后的数据进行局部聚合
        JavaPairRDD<String, Long> randomReduceRDD = randomRDD.reduceByKey((x, y) -> x + y);
        //去掉随机前缀
        JavaPairRDD<String, Long> resRdd = randomReduceRDD.mapToPair(e -> {
            String[] str = e._1.split("_");
            return new Tuple2<String, Long>(str[1], e._2);
        });
        JavaPairRDD<String, Long> pairRDD = resRdd.reduceByKey((x, y) -> x + y);
        pairRDD.foreach(e -> System.out.println(e._1 + "-" + e._2));
        //和不包含数据倾斜最多key的RDD进行聚合
        JavaPairRDD<String, Long> resultRDD = pairRDD.join(commonRDD).mapToPair(e -> {
            return new Tuple2<>(e._1, e._2._1);
        });
    }
}
