package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final Logger log = LoggerFactory.getLogger(MyReducer.class);

    private LongWritable v3 = new LongWritable();

    /**
     * 需要实现reduce函数
     * 针对多个<k2, {v2.....}>的数据累加求和，并把数据转化成<k3, v3>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        log.info("K2,V2的值为：<" + key.toString() + ", {" + values+ "}>");
        long sum = 0L;
        for (LongWritable v : values){
            sum += v.get();
        }
        v3.set(sum);
        context.write(key, v3);
        log.info("K3,V3的值为：<" + key.toString() + ", " + v3.get()+ ">");
    }
}
