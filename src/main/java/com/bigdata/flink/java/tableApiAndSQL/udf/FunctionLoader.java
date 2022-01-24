package com.bigdata.flink.java.tableApiAndSQL.udf;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.ServiceLoader;

/**
 * 自定义函数(UDF)类加载器
 * ServiceLoader 因为是通过反射进行加载的，所有的接口实现类必须拥有一个无参构造函数，否则实现类无法被反射实例化
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
