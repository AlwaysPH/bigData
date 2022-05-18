package com.flink.task.impl;

import com.flink.model.VehJobInfo;
import com.flink.task.BaseJob;
import com.flink.utils.JdbcUtils;
import com.flink.utils.MysqlConfigUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Properties;

/**
 * flink连接mysql
 * @author 00074964
 * @version 1.0
 * @date 2022-5-17 19:32
 */
public class ReadMySqlData extends BaseJob {

    @Override
    protected void runJob(StreamExecutionEnvironment env) throws Exception {
        String querySql = "SELECT\n" +
                "\tt.CID,\n" +
                "\tt.PROJECT_ID,\n" +
                "\tt.VEH_ID,\n" +
                "\tt.WORK_DATE,\n" +
                "\tt.TODAY_TOTAL_FUEL,\n" +
                "\tt.TODAY_WORK_FUEL,\n" +
                "\tt.TODAY_TOTAL_MILEAGE,\n" +
                "\tt.TODAY_WORK_MILEAGE,\n" +
                "\tt.TODAY_WORK_TIMES,\n" +
                "\tt.TODAY_TOTAL_TIMES,\n" +
                "\tt.FIRST_FUEL,\n" +
                "\tt.LAST_FUEL,\n" +
                "\tt.FIRST_GPS_TIME,\n" +
                "\tt.LAST_GPS_TIME,\n" +
                "\tt.FIRST_MILEAGE,\n" +
                "\tt.LAST_MILEAGE,\n" +
                "\tt.ADD_WATER_COUNT,\n" +
                "\tt.COLLECT_NUMBER,\n" +
                "\tt.ADD_WATER_AVG_MILEAGE,\n" +
                "\tt.WORK_AREA,\n" +
                "\tt.AVG_ADD_WATER_WORK_AREA,\n" +
                "\tt.FIRST_COLLECT_TIME,\n" +
                "\tt.END_COLLECT_TIME,\n" +
                "\tt.FIRST_UNLOAD_TIME,\n" +
                "\tt.END_UNLOAD_TIME,\n" +
                "\tt.TOTAL_COLLECT,\n" +
                "\tt.TRANSPORT_NUM,\n" +
                "\tt.ONLINE_TIMES,\n" +
                "\tt.VEH_TYPE,\n" +
                "\tv.VBI_LICENSE AS vbiLicense,\n" +
                "\tp.PROJECT_NAME AS projectName,\n" +
                "\tTDD.DATA_NAME AS vehClassName,\n" +
                "\tTDD1.DATA_NAME AS vehSecondClassName\n" +
                "FROM\n" +
                "t_vehicle_today_totaljob_info t\n" +
                "INNER JOIN veh_base_info v ON t.veh_id = v.id\n" +
                "AND v.delete_flag = 0\n" +
                "LEFT JOIN t_project_info p ON t.project_id = p.id\n" +
                "AND p.delete_flag = 0\n" +
                "INNER JOIN veh_type_info vf ON v.vti_id = vf.id\n" +
                "INNER JOIN t_data_dictionary tdd ON tdd.data_type = 'VEH_CLASS'\n" +
                "AND tdd.data_code = vf.veh_class\n" +
                "AND tdd.delete_flag = 0\n" +
                "INNER JOIN t_data_dictionary tdd1 ON tdd1.data_type = 'VEH_SECOND_CLASS'\n" +
                "AND vf.veh_second_class = tdd1.data_code\n" +
                "AND tdd1.enable_flag = 0\n" +
                "AND tdd1.delete_flag = 0\n" +
                "AND tdd.subsyscode = 'ljqyxt'\n" +
                "WHERE\n" +
                "\tt.`WORK_DATE` >= '2022-05-17'\n" +
                "AND '2022-05-17' >= t.`WORK_DATE`\n" +
                "ORDER BY\n" +
                "\tt.WORK_DATE DESC,\n" +
                "\tt.CID DESC\n";
        DataStreamSource<VehJobInfo> source = env.addSource(new MysqlSource(querySql)).setParallelism(1);
        source.print();
        env.execute();
//        source.keyBy(VehJobInfo::getWorkDate).timeWindow(Time.hours(2), Time.minutes(10)).aggregate(new AdCountAgg());
    }

    public class MysqlSource extends RichSourceFunction<VehJobInfo>{

        private final String sql;

        private QueryRunner queryRunner;

        public MysqlSource(String sql) {
            this.sql = sql;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Properties properties = MysqlConfigUtil.buildMysqlProps();
            queryRunner = new QueryRunner(JdbcUtils.getDataSource(properties));
        }

        @Override
        public void run(SourceContext<VehJobInfo> ctx) throws Exception {
            VehJobInfo jobInfo= queryRunner.query(sql, new BeanHandler<>(VehJobInfo.class));
            ctx.collect(jobInfo);
        }

        @Override
        public void cancel() {

        }

    }

    private class AdCountAgg implements AggregateFunction<VehJobInfo, Long, Long>{
        @Override
        public Long createAccumulator() {
            return null;
        }

        @Override
        public Long add(VehJobInfo vehJobInfo, Long aLong) {
            return null;
        }

        @Override
        public Long getResult(Long aLong) {
            return null;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
}
