package com.spark.java.sparkSql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadJsonTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("ReadJsonTest")
                .master("local[*]")
                .getOrCreate();

        //通过sparkSession获取JavaSparkContext
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Dataset<Row> json = sparkSession.read().json("data\\video_info.log");

        json.createOrReplaceTempView("video_info");
        sparkSession.sql("select *from video_info").show();

        //dataFrame转成RDD
        JavaRDD<Row> rowJavaRDD = json.toJavaRDD();


        sparkSession.stop();
    }
}
