package com.flink.utils;

import com.flink.model.VehJobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Properties;

/**
 * 数据入库到JDBC
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 15:35
 */
@Slf4j
public class SinkToJdbcUtils extends RichSinkFunction<List<VehJobInfo>> {

    private final ParameterTool parameterTool;

    private QueryRunner queryRunner;

    public SinkToJdbcUtils(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = MysqlConfigUtils.buildMysqlProps(parameterTool);
        queryRunner = new QueryRunner(JdbcUtils.getDataSource(properties));
    }

    @Override
    public void invoke(List<VehJobInfo> value, Context context) throws Exception {
        try {
            StringBuilder sb = new StringBuilder("BEGIN;");
            value.forEach(data -> {
                sb.append("update t_vehicle_today_totaljob_info " +
                        "set TODAY_TOTAL_MILEAGE = " + data.getTodayTotalMileage() + "," +
                        "TODAY_TOTAL_TIMES = " + data.getTodayTotalTimes() + "," +
                        "TOTAL_COLLECT = " + data.getTotalCollect() + "," +
                        "TRANSPORT_NUM = " + data.getTransportNum() + "," +
                        "ONLINE_TIMES = " + data.getOnlineTimes() + " where " +
                        " VEH_ID = " + data.getVehId() + " and WORK_DATE = " + data.getWorkDate() + ";");
            });
            sb.append("COMMIT;");
            queryRunner.update(sb.toString());
        }catch (Exception e){
            log.error("保存垃圾收运数据失败", e);
        }

    }

    @Override
    public void close() throws Exception {

    }
}
