package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        String key = text.toString();
        switch (key){
            case "152" : numPartitions = 0; break;
            case "137" : numPartitions = 1; break;
            case "150" : numPartitions = 2; break;
            case "185" : numPartitions = 3; break;
            default: numPartitions = 4; break;
        }
        return numPartitions;
    }
}
