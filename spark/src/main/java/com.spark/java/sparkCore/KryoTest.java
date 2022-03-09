package com.spark.java.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class KryoTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("KryoTest")
                .setMaster("local")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.classesToRegister", "com.spark.java.sparkCore.Person");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("xiaohei xiaohong", "xiaoming xiaochou");
        JavaRDD<String> rdd = sc.parallelize(list);
        JavaRDD<String> data = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        JavaRDD<Person> resultRDD = data.map(e -> new Person(e, 18)).persist(StorageLevel.MEMORY_ONLY_SER());
        resultRDD.foreach(e -> System.out.println(e.toString()));
        while(true){
            ;
        }
    }
}

class Person implements Serializable {
    private static final long serialVersionUID = 7727496832015252122L;

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    private String name;

    private Integer age;

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
