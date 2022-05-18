package com.flink.utils;

import com.flink.common.PropertiesConstants;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * JDBC连接
 * @author 00074964
 * @version 1.0
 * @date 2022-5-18 14:02
 */
public class JdbcUtils {

    private static BasicDataSource dataSource = new BasicDataSource();

    public static DataSource getDataSource(Properties properties) {
        //数据库连接信息配置
        dataSource.setDriverClassName(properties.getProperty(PropertiesConstants.MYSQL_DRIVER));
        dataSource.setUrl(properties.getProperty(PropertiesConstants.MYSQL_URL));
        dataSource.setUsername(properties.getProperty(PropertiesConstants.MYSQL_USERNAME));
        dataSource.setPassword(properties.getProperty(PropertiesConstants.MYSQL_PASSWORD));
        //初始化的连接数
        dataSource.setInitialSize(10);
        //最大连接数量
        dataSource.setMaxActive(8);
        //最大空闲数
        dataSource.setMaxIdle(5);
        //最小空闲数
        dataSource.setMinIdle(1);
        return dataSource;
    }
}
