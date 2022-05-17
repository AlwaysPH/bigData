package com.flink.task.impl;

import com.flink.task.BaseJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * flink连接mysql
 * @author 00074964
 * @version 1.0
 * @date 2022-5-17 19:32
 */
public class ReadMySqlData extends BaseJob {

    @Override
    protected void runJob(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Object> source = env.addSource(new MysqlSource());
    }

    public class MysqlSource extends RichSourceFunction<Object>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<Object> ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}
