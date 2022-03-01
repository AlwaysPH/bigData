package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsTest {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigData01:9000");
        FileSystem fileSystem = FileSystem.get(configuration);

        //HDFS上传文件
//        put(fileSystem);

        //HDFS下载文件
//        download(fileSystem);

        //HDFS删除文件
        deleteFile(fileSystem);
        return;
    }

    private static void deleteFile(FileSystem fileSystem) throws IOException {
        if(!fileSystem.exists(new Path("/hadoop-3.2.0.tar.gz"))){
            System.out.println("文件不存在");
        }
        boolean flag = fileSystem.delete(new Path("/hadoop-3.2.0.tar.gz"), true);
        if(flag){
            System.out.println("删除成功");
        }else {
            System.out.println("删除失败");
        }
    }

    private static void download(FileSystem fileSystem) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/README.txt"));
        FileOutputStream outputStream = new FileOutputStream("E:\\README.txt");
        IOUtils.copyBytes(inputStream, outputStream, 1024, true);
    }

    public static void put(FileSystem fileSystem) throws IOException {
        FileInputStream inputStream = new FileInputStream("E:\\test.txt");
        //获取HDFS文件输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test.txt"));
        //上传文件
        IOUtils.copyBytes(inputStream, outputStream, 1024, true);
    }
}
