package com.flink.task;

import com.flink.utils.ExecutionEnvUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-14 11:47
 */
@Log4j2
public abstract class BaseJob {

    private ParameterTool parameterTool = null;
    private StreamExecutionEnvironment env = null;

    /**
     * 子类需实现的方法
     * @param env
     * @throws Exception
     */
    protected abstract void runJob(StreamExecutionEnvironment env) throws  Exception;

    private void init(){
        try {
            parameterTool =  ExecutionEnvUtil.createParameterTool();
            env = ExecutionEnvUtil.prepare(parameterTool);
        } catch (Exception e) {
            log.error("Init Stream execution environment failed!");
        }
    }
}
