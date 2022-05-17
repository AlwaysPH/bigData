package com.flink.common;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-3-14 11:48
 */
public class PropertiesConstants {

    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "zhisheng";
    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String CHECKPOINT_MEMORY = "memory";
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb";

    /**
     * es
     */
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    /**
     * redis
     */
    public static final String REDIS_HOSTS = "redis.hosts";
    public static final String REDIS_MASTER = "redis.mater";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_POOL_MAXIDEL = "redis.pool.maxIdel";
    public static final String REDIS_POOL_MAXTOTAL = "redis.maxTotal";
    public static final String REDIS_TIMEOUT = "redis.timeout";

    /**
     * mysql
     */
    public static final String MYSQL_URL = "mysql.jdbc.connectionURL";
    public static final String MYSQL_USERNAME = "mysql.jdbc.username";
    public static final String MYSQL_PASSWORD = "mysql.jdbc.password";
}
