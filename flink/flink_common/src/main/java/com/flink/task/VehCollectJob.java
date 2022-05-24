package com.flink.task;

import com.flink.function.MysqlSourceFunction;
import com.flink.function.VehCollectFlatMapFunction;
import com.flink.model.VehJobInfo;
import com.flink.model.enums.DateEnum;
import com.flink.model.req.QueryParams;
import com.flink.utils.DateUtils;
import com.flink.utils.ExecutionEnvUtils;
import com.flink.utils.SinkToJdbcUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.List;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-18 15:48
 */
public class VehCollectJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool =  ExecutionEnvUtils.createParameterTool();
        StreamExecutionEnvironment env = ExecutionEnvUtils.prepare(parameterTool);
        //每隔30秒checkpoint一次
//        env.enableCheckpointing(30 * 1000L);
//        //checkpoint模式
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //checkpoint的存储位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata01:9000/flink/checkData");
//        //checkpoint超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        //两次checkpoint之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5 * 1000L);
//        //checkpoint并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        String querySql = "SELECT " +
                "t.CID as id, " +
                "t.PROJECT_ID as projectId, " +
                "t.VEH_ID as vehId, " +
                "t.WORK_DATE as workDate, " +
                "t.TODAY_TOTAL_FUEL as todayTotalFuel, " +
                "t.TODAY_WORK_FUEL as  todayWorkFuel, " +
                "t.TODAY_TOTAL_MILEAGE as todayTotalMileage, " +
                "t.TODAY_WORK_MILEAGE as todayWorkMileage, " +
                "t.TODAY_WORK_TIMES as todayWorkTimes, " +
                "t.TODAY_TOTAL_TIMES as todayTotalTimes, " +
                "t.FIRST_FUEL as firstFuel, " +
                "t.LAST_FUEL as lastFuel, " +
                "t.FIRST_GPS_TIME as firstGpsTime, " +
                "t.LAST_GPS_TIME as lastGpsTime, " +
                "t.FIRST_MILEAGE as firstMileage, " +
                "t.LAST_MILEAGE as lastMileage, " +
                "t.ADD_WATER_COUNT as addWaterCount, " +
                "t.COLLECT_NUMBER as collectNumber, " +
                "t.ADD_WATER_AVG_MILEAGE as addWaterAvgMileage, " +
                "t.WORK_AREA as workArea, " +
                "t.TOTAL_COLLECT as totalCollect, " +
                "t.TRANSPORT_NUM as transportNum, " +
                "t.ONLINE_TIMES as onlineTimes, " +
                "t.VEH_TYPE as vehType, " +
                "v.VBI_LICENSE AS vbiLicense, " +
                "p.PROJECT_NAME AS projectName, " +
                "TDD.DATA_NAME AS vehClassName, " +
                "TDD1.DATA_NAME AS vehSecondClassName " +
                "FROM " +
                "t_vehicle_today_totaljob_info t " +
                "INNER JOIN veh_base_info v ON t.veh_id = v.id " +
                "AND v.delete_flag = 0 " +
                "LEFT JOIN t_project_info p ON t.project_id = p.id " +
                "AND p.delete_flag = 0 " +
                "INNER JOIN veh_type_info vf ON v.vti_id = vf.id " +
                "INNER JOIN t_data_dictionary tdd ON tdd.data_type = 'VEH_CLASS' " +
                "AND tdd.data_code = vf.veh_class " +
                "AND tdd.delete_flag = 0 " +
                "INNER JOIN t_data_dictionary tdd1 ON tdd1.data_type = 'VEH_SECOND_CLASS' " +
                "AND vf.veh_second_class = tdd1.data_code " +
                "AND tdd1.enable_flag = 0 " +
                "AND tdd1.delete_flag = 0 " +
                "AND tdd.subsyscode = 'ljqyxt' " +
                "WHERE " +
                "t.`WORK_DATE` >= ? " +
                "AND ? >= t.`WORK_DATE` " +
                "ORDER BY " +
                "t.WORK_DATE DESC, " +
                "t.CID DESC";
        String time = DateUtils.parseToString(new Date(), DateEnum.YEAR_MONTH_DAY.getType());
        QueryParams params = new QueryParams("2022-05-17", "2022-05-17");
        DataStreamSource<List<VehJobInfo>> source = env.addSource(new MysqlSourceFunction(querySql, parameterTool, params));
        source.print("query");
        SingleOutputStreamOperator<List<VehJobInfo>> flatMap = source.keyBy(e -> e.get(0).getVehId())
                .flatMap(new VehCollectFlatMapFunction());
        flatMap.print("update");
//        flatMap.addSink(new SinkToJdbcUtils(parameterTool));
        env.execute();
    }
}
