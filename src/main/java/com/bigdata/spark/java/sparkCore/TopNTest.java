package com.bigdata.spark.java.sparkCore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class TopNTest {

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();
        JavaRDD<String> video = sc.textFile("hdfs://bigData01:9000/data/topN/video_info.log");
        JavaRDD<String> gift = sc.textFile("hdfs://bigData01:9000/data/topN/gift_record.log");

        JavaPairRDD<String, Tuple2<String, String>> videoData = video.mapToPair(e -> {
            JSONObject jsonObject = JSON.parseObject(e);
            String uid = jsonObject.getString("uid");
            String vid = jsonObject.getString("vid");
            String area = jsonObject.getString("area");
            Tuple2<String, String> st = new Tuple2<>(uid, area);
            Tuple2<String, Tuple2<String, String>> res = new Tuple2<String, Tuple2<String, String>>(vid, st);
            return res;
        });

        JavaPairRDD<String, Integer> giftData = gift.mapToPair(e -> {
            JSONObject jsonObject = JSON.parseObject(e);
            String vid = jsonObject.getString("vid");
            Integer gold = Integer.valueOf(jsonObject.getString("gold"));
            return new Tuple2<String, Integer>(vid, gold);
        });

        JavaPairRDD<String, Integer> giftRDD = giftData.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Tuple2<String, String>, Integer> mapRDD = videoData.join(giftRDD).mapToPair(e -> {
            String uid = e._2._1._1;
            String area = e._2._1._2;
            Integer gold = e._2._2;
            return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(uid, area), gold);
        });

        JavaPairRDD<Tuple2<String, String>, Integer> reRDD = mapRDD.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = reRDD.mapToPair(e -> new Tuple2<String, Tuple2<String, Integer>>(e._1._2, new Tuple2<String, Integer>(e._1._1, e._2))).groupByKey();

        JavaRDD<Tuple2<String, String>> result = groupRDD.map(e -> {
            ArrayList<Tuple2<String, Integer>> list = Lists.newArrayList(e._2);
            Collections.sort(list, new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    return o2._2 - o1._2;
                }
            });
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                if (i < 3) {
                    Tuple2<String, Integer> st = list.get(i);
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(st._1 + ":" + st._2);
                }
            }
            return new Tuple2<String, String>(e._1, sb.toString());
        });

        result.saveAsTextFile("hdfs://bigData01:9000/data/topN/result");
        sc.stop();
    }

    public static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("TopNTest");
//                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
