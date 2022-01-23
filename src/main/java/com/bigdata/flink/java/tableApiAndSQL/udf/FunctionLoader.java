package com.bigdata.flink.java.tableApiAndSQL.udf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.ServiceLoader;

/**
 * 自定义函数(UDF)类加载器
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class FunctionLoader {

    public static FunctionLoader INSTANCE = new FunctionLoader();

    private static ServiceLoader<Function> serviceLoader  = ServiceLoader.load(Function.class);

    private FunctionLoader() {}

    public List<Function> discoverFunctions() {
        return Lists.newArrayList(serviceLoader.iterator());
    }

}
