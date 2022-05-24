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
            String sql = "update t_vehicle_today_totaljob_info set TODAY_TOTAL_MILEAGE = ?, TODAY_TOTAL_TIMES = ?," +
                    " TOTAL_COLLECT = ?, TRANSPORT_NUM = ?, ONLINE_TIMES = ?, update_time = ? " +
                    "where VEH_ID = ? and WORK_DATE = ?";
            Object[][] params = new Object[value.size()][8];
            for (int i = 0; i < value.size(); i++) {
                VehJobInfo info = value.get(i);
                params[i][0] = info.getTodayTotalMileage();
                params[i][1] = info.getTodayTotalTimes();
                params[i][2] = info.getTotalCollect();
                params[i][3] = info.getTransportNum();
                params[i][4] = info.getOnlineTimes();
                params[i][5] = info.getUpdateTime();
                params[i][6] = info.getVehId();
                params[i][7] = info.getWorkDate();
            }
            queryRunner.batch(sql, params);
        }catch (Exception e){
            log.error("保存垃圾收运数据失败", e);
        }

    }

    @Override
    public void close() throws Exception {

    }
}
