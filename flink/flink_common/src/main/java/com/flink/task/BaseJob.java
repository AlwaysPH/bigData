package com.flink.task;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-14 11:47
 */
public abstract class BaseJob {

    protected abstract void runJob() throws  Exception;

    protected abstract void init() throws Exception;
}
