package com.bigdata.spark.java.sparkSql;

import org.apache.spark.sql.SparkSession;

public class SparkUtil {

    public static class SingletonClassInstance{
        private static final SparkUtil instance = new SparkUtil();
    }

    public static SparkUtil getInstance(){
        return SingletonClassInstance.instance;
    }

    public SparkSession getSparkSession(String appName, String master){
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
    }
}
