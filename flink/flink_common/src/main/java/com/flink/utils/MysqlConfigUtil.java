package com.flink.utils;

import com.flink.common.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * mysql配置类
 * @author 00074964
 * @version 1.0
 * @date 2022-5-18 11:52
 */
public class MysqlConfigUtil {

    public static Properties buildMysqlProps() {
        return buildMysqlProps(ParameterTool.fromSystemProperties());
    }

    public static Properties buildMysqlProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("driverClass", parameterTool.get(PropertiesConstants.MYSQL_DRIVER));
        props.put("connectionURL", parameterTool.get(PropertiesConstants.MYSQL_URL));
        props.put("username", parameterTool.get(PropertiesConstants.MYSQL_USERNAME));
        props.put("password", parameterTool.get(PropertiesConstants.MYSQL_PASSWORD));
        return props;
    }
}
