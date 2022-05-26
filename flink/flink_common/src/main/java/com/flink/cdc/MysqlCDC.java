package com.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.flink.common.PropertiesConstants;
import com.flink.utils.ExecutionEnvUtils;
import com.flink.utils.MysqlConfigUtils;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Properties;

/**
 * @author ph
 * @version 1.0
 * @description 监测mysql指定数据库数据变化
 * @date 2022-05-25 15:23
 */
public class MysqlCDC {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool =  ExecutionEnvUtils.createParameterTool();
        StreamExecutionEnvironment env = ExecutionEnvUtils.prepare(parameterTool);
        env.setParallelism(1);

//        Properties properties = new Properties();
//        // 配置 Debezium在初始化快照的时候（扫描历史数据的时候） =》 不要锁表
//        properties.setProperty("debezium.snapshot.locking.mode", "none");
//        //配置flink checkpoint
//        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));
//        //最大同时存在的ck数 和设置的间隔时间有一个就行
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.SECONDS.toMillis(5));
//        //指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 5000L));
//        //设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties mysqlProps = MysqlConfigUtils.buildMysqlProps(parameterTool);
        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname(mysqlProps.getProperty(PropertiesConstants.MYSQL_HOST))
                .port(Integer.valueOf(mysqlProps.getProperty(PropertiesConstants.MYSQL_PORT)))
                .username(mysqlProps.getProperty(PropertiesConstants.MYSQL_USERNAME))
                .password(mysqlProps.getProperty(PropertiesConstants.MYSQL_PASSWORD))
                //指定监测数据库，可以配置多个（逗号分隔）
                .databaseList("bigdata")
                //指定数据库表
                .tableList("bigdata.t_vehicle_today_totaljob_info")
                //自定义反序列化
                .deserializer(new myDeserializationSchema())
                .startupOptions(StartupOptions.initial())
//                .debeziumProperties(properties)
                .build();

        /*
         *  .startupOptions(StartupOptions.latest()) 参数配置
         *  1.initial() 全量扫描并且继续读取最新的binlog 最佳实践是第一次使用这个
         *  2.earliest() 从binlog的开头开始读取 就是啥时候开的binlog就从啥时候读
         *  3.latest() 从最新的binlog开始读取
         *  4.specificOffset(String specificOffsetFile, int specificOffsetPos) 指定offset读取
         *  5.timestamp(long startupTimestampMillis) 指定时间戳读取
         */
        DataStreamSource<String> dataStreamSource = env.addSource(source);
        dataStreamSource.print();
        env.execute();

    }

    private static class myDeserializationSchema implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            JSONObject result = new JSONObject();

            String[] split = sourceRecord.topic().split("\\.");
            result.put("db",split[1]);
            result.put("tb",split[2]);
            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            result.put("op",operation.toString().toLowerCase());
            Struct value =(Struct)sourceRecord.value();
            JSONObject after = getValueBeforeAfter(value, "after");
            JSONObject before = getValueBeforeAfter(value, "before");
            if (after!=null){
                result.put("after",after);
            }
            if (before!=null){
                result.put("before",before);
            }
            collector.collect(result.toJSONString());
        }

        private JSONObject getValueBeforeAfter(Struct value, String type) {
            Struct struct = value.getStruct(type);
            JSONObject result = new JSONObject();
            if(null != struct){
                List<Field> fields = struct.schema().fields();
                fields.forEach(e -> {
                    result.put(e.name(), struct.get(e.name()));
                });
            }
            return result;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
