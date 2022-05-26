package com.flink.cdc;

import com.flink.common.PropertiesConstants;
import com.flink.utils.ExecutionEnvUtils;
import com.flink.utils.MysqlConfigUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author ph
 * @version 1.0
 * @description TODO
 * @date 2022-05-26 09:59
 */
public class SqlCDC {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool =  ExecutionEnvUtils.createParameterTool();
        StreamExecutionEnvironment env = ExecutionEnvUtils.prepare(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        Properties mysqlProps = MysqlConfigUtils.buildMysqlProps(parameterTool);
        tEnv.executeSql("create table t_job_info(" +
                "CID int not null primary key," +
                "PROJECT_ID int," +
                "VEH_ID int," +
                "WORK_DATE int," +
                "TODAY_TOTAL_FUEL decimal(15,2)," +
                "TODAY_WORK_FUEL decimal(15,2)," +
                "TODAY_TOTAL_MILEAGE decimal(10,2)," +
                "TODAY_WORK_MILEAGE decimal(10,2)," +
                "TODAY_WORK_TIMES decimal(10,2)," +
                "TODAY_TOTAL_TIMES decimal(10,2)," +
                "FIRST_FUEL decimal(15,2)," +
                "LAST_FUEL decimal(15,2)," +
                "FIRST_GPS_TIME timestamp," +
                "LAST_GPS_TIME timestamp," +
                "FIRST_MILEAGE decimal(10,2)," +
                "LAST_MILEAGE decimal(10,2)," +
                "ADD_WATER_COUNT int," +
                "COLLECT_NUMBER int," +
                "ADD_WATER_AVG_MILEAGE decimal(10,2)," +
                "WORK_AREA decimal(10,2)," +
                "AVG_ADD_WATER_WORK_AREA decimal(10,2)," +
                "FIRST_COLLECT_TIME timestamp," +
                "END_COLLECT_TIME timestamp," +
                "FIRST_UNLOAD_TIME timestamp," +
                "END_UNLOAD_TIME timestamp," +
                "TOTAL_COLLECT decimal(10,2)," +
                "TRANSPORT_NUM int," +
                "COLLECT_POINT_NUM int," +
                "ONLINE_TIMES decimal(10,2)," +
                "VEH_TYPE int," +
                "WORK_NUM int," +
                "CREATE_TIME timestamp," +
                "VEH_CLASS int," +
                "update_time timestamp" +
                ") with (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '" + mysqlProps.getProperty(PropertiesConstants.MYSQL_HOST) + "'," +
                "'port' = '" + mysqlProps.getProperty(PropertiesConstants.MYSQL_PORT) + "'," +
                "'username' = '" + mysqlProps.getProperty(PropertiesConstants.MYSQL_USERNAME) + "'," +
                "'password' = '" + mysqlProps.getProperty(PropertiesConstants.MYSQL_PASSWORD) + "'," +
                "'database-name' = 'bigdata'," +
                "'table-name' = 't_vehicle_today_totaljob_info'" +
                ")");
        Table table = tEnv.sqlQuery("select * from t_job_info");
        DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(table, Row.class);
        dataStream.print();
        env.execute();
    }
}
