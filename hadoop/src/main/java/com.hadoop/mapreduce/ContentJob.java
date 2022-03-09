package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class ContentJob {

    public static void main(String[] args) {
        try {
            //创建JOB需要的配置参数
            Configuration config = new Configuration();
            //解析通过命令行-D传递过来的参数，添加到config中
            String[] remains = new GenericOptionsParser(config, args).getRemainingArgs();
            //创建一个job
            Job job = Job.getInstance(config);
            job.setJarByClass(ContentJob.class);

            //指定输入路径（可以是文件、目录）
            FileInputFormat.setInputPaths(job, new Path(remains[0]));
            //指定输出路径（只能指定一个不存在的目录）
            FileOutputFormat.setOutputPath(job, new Path(remains[1]));

            //mapper
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            //reducer
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //自定义分区需要指定
            job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(5);

            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
