package com.spark.java.sparkSql;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RDDToDataFrameTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDToDataFrameTest")
                .master("local[*]")
                .getOrCreate();

        //通过sparkSession获取JavaSparkContext
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        List<Tuple2<String, Integer>> list = Lists.newArrayList(
                new Tuple2<>("xiaoming", 18),
                new Tuple2<>("xiaohong", 22),
                new Tuple2<>("xiaohei", 19)
        );

        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list);
        JavaRDD<Row> rowRDD = rdd.map(e -> RowFactory.create(e._1, e._2));

        ArrayList<StructField> structFieldList = new ArrayList<>();
        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField ageField = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        structFieldList.add(nameField);
        structFieldList.add(ageField);

        StructType schema = DataTypes.createStructType(structFieldList);

        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, schema);

        dataFrame.createOrReplaceTempView("student");

        sparkSession.sql("select name, age from student where age > 18").show();

        sparkSession.stop();
    }
}
