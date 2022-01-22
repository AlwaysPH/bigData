package com.bigdata.flink.java.tableApiAndSQL;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TableEnv环境创建
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableEnv {
    public static void main(String[] args) {

        /**基于流处理**/
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定执行引擎为blink，以及数据处理模式为-stream
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //创建TableEnvironment对象
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, settings);


        /**基于批处理**/
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        //指定执行引擎为blink，以及数据处理模式为-batch
        EnvironmentSettings bSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        //创建TableEnvironment对象
        TableEnvironment bEnv = TableEnvironment.create(bSettings);


        /**
         * 如果table api和SQL需要和DataStream或DataSet进行转换
         * 针对stream需要使用
         */
        //创建StreamTableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment environment = StreamTableEnvironment.create(env, ssSetting);

        //创建batchTableEnvironment
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(executionEnvironment);

    }
}
