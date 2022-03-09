package com.livy.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.livy.utils.HttpUtil;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-8 17:24
 */
public class LivyContext {

    @Value("${livy.host}")
    private static String host;

    @Value("${spark.master}")
    private static String master;

    @Value("${spark.submit.deployMode}")
    private static String deployMode;

    @Value("${livy.kind}")
    private static String kind;

    @Value("${livy.proxyUser}")
    private static String proxyUser;

    @Value("${livy.file}")
    private static String file;

    @Value("${livy.jars}")
    private static String[] jars;

    @Value("${livy.className}")
    private static String className;

    @Value("${livy.name}")
    private static String name;

    @Value("${livy.executorCores}")
    private static Integer executorCores;

    @Value("${livy.executorMemory}")
    private static String executorMemory;

    @Value("${livy.driverCores}")
    private static Integer driverCores;

    @Value("${livy.driverMemory}")
    private static String driverMemory;

    @Value("${livy.numExecutors}")
    private static Integer numExecutors;

    @Value("${livy.queue}")
    private static String queue;

    /**
     * livy提交job
     * @return
     */
    public static Integer livySubmit(){
        JSONObject livyConf = new JSONObject();
        //spark配置
        Map<String, Object> sparkConf = Maps.newHashMap();
        sparkConf.put("spark.master", master);
        sparkConf.put("spark.submit.deployMode", deployMode);

        livyConf.put("conf", sparkConf);
        livyConf.put("proxyUser", proxyUser);
        livyConf.put("file", file);// 指定执行的spark jar (hdfs路径)
        livyConf.put("jars", jars);//指定spark jar依赖的外部jars
        livyConf.put("className", className);
        livyConf.put("name", name);
        livyConf.put("executorCores", executorCores);
        livyConf.put("executorMemory", executorMemory);
        livyConf.put("driverCores", driverCores);
        livyConf.put("driverMemory", driverMemory);
        livyConf.put("numExecutors", numExecutors);
        livyConf.put("queue", queue);
        livyConf.put("args",new String[]{"杭州","yj_hangzhou","2019041719"});//传递参数

        String res = HttpUtil.postAccess(host + "/batches", livyConf);
        JSONObject resjson = JSON.parseObject(res);
        return resjson.getIntValue("id");
    }

    /**
     * 获取livy提交任务结果状态信息
     * @param id
     */
    public static void getJobInfo(int id){
        JSONObject state = HttpUtil.getAccess(host + "/batches/"+id+"/state");
        System.out.println(state.getString("state"));
    }

    /**
     * 取消spark任务
     * @param id
     */
    public static void killJob(int id){
        HttpUtil.deleteAccess(host+"/batches/"+id);
    }
}
