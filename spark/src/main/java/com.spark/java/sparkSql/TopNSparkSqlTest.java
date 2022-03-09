package com.spark.java.sparkSql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopNSparkSqlTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtil.getInstance().getSparkSession("TopNSparkSqlTest", "local[*]");

        Dataset<Row> gift = sparkSession.read().json("data\\gift_record.log");
        Dataset<Row> video = sparkSession.read().json("data\\video_info.log");

        gift.createOrReplaceTempView("gift_record");
        video.createOrReplaceTempView("video_info");

        Dataset<Row> giftDataSet = sparkSession.sql("select vid, sum(gold) gold from gift_record group by vid");

        giftDataSet.createOrReplaceTempView("giftDataSet");

        Dataset<Row> result = sparkSession.sql("select c.area, concat_ws(',', collect_list(c.topN)) as topN from(select b.area, concat(b.uid, ':', b.gold) as topN from(select a.area, a.uid, a.gold, row_number () over (partition BY area order by " +
                "gold desc) num from( " +
                "select t1.area, t1.uid, sum(t2.gold) gold " +
                "from video_info t1 LEFT JOIN giftDataSet t2 ON t1.vid = t2.vid GROUP BY t1.area, t1.uid ORDER BY gold DESC) a) b where b.num <=3) c group by c.area");

        JavaRDD<Row> rowJavaRDD = result.toJavaRDD();
        rowJavaRDD.foreach(e -> System.out.println(e));

        sparkSession.stop();

    }
}
