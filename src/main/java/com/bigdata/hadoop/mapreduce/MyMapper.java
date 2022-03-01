package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger log = LoggerFactory.getLogger(MyMapper.class);

    private Text text = new Text();

    private LongWritable v2 = new LongWritable(1L);

    /**
     * 需要实现map函数
     * 函数接收<k1, v1>，产生<k2, v2>
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        log.info("K1,V1的值为：<" + key + ", " + value+ ">");
        String[] values = value.toString().split(" ");
        for(String i : values){
            text.set(i);
            context.write(text, v2);
            log.info("K2,V2的值为：<" + text.toString() + ", " + v2.get()+ ">");
        }
    }
}
