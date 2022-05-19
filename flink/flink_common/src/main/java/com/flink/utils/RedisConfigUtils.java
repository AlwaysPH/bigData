package com.flink.utils;

import com.flink.common.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;


/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-14 15:15
 */
public class RedisConfigUtils {

    public static FlinkJedisPoolConfig buildRedisConfig(){
        return buildRedisConfig(ParameterTool.fromSystemProperties());
    }

    public static FlinkJedisPoolConfig buildRedisConfig(ParameterTool parameterTool){
        String redisHosts = parameterTool.get(parameterTool.get(PropertiesConstants.KAFKA_BROKERS));
        FlinkJedisPoolConfig redisProduceConfig = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHosts)
                .setPassword(parameterTool.get(PropertiesConstants.REDIS_PASSWORD))
                .setMaxIdle(parameterTool.getInt(PropertiesConstants.REDIS_POOL_MAXIDEL))
                .setMaxTotal(parameterTool.getInt(PropertiesConstants.REDIS_POOL_MAXTOTAL))
                .setTimeout(parameterTool.getInt(PropertiesConstants.REDIS_TIMEOUT)).build();
        return redisProduceConfig;
    }
}
