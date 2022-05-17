package com.flink.task.impl;

import com.flink.task.BaseJob;
import com.flink.utils.ExecutionEnvUtil;
import com.flink.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-14 11:50
 */
public class WordCountJob extends BaseJob {

    private ParameterTool parameterTool = null;

    private StreamExecutionEnvironment env = null;

    @Override
    protected void runJob() throws Exception {
        DataStreamSource<?> source = KafkaConfigUtil.buildSource(env);
    }

    @Override
    protected void init() throws Exception {
        parameterTool =  ExecutionEnvUtil.createParameterTool();
        env = ExecutionEnvUtil.prepare(parameterTool);
    }
}
