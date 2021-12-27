package com.bigdata.spark.java.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JDBCTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("JDBCTest")
                .master("local[*]")
                .getOrCreate();

        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "888888");

        Dataset<Row> order = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/cas?useUnicode=true&characterEncoding=utf8&useSSL=true&serverTimezone=UTC", "t_order", properties);

        order.createOrReplaceTempView("t_order");

        sparkSession.sql("select *from t_order").show();

        sparkSession.stop();

    }
}
