package com.flink.function;

import com.flink.model.VehJobInfo;
import com.flink.model.req.QueryParams;
import com.flink.utils.JdbcUtils;
import com.flink.utils.MysqlConfigUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 自定义mysql数据源
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 14:29
 */
public class MysqlSourceFunction extends RichSourceFunction<List<VehJobInfo>> {

    private final String sql;

    private final ParameterTool parameterTool;

    private final QueryParams params;

    private QueryRunner queryRunner;

    /**
     * 定时器间隔时间(ms)
     */
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    public MysqlSourceFunction(String sql, ParameterTool parameterTool, QueryParams params) {
        this.sql = sql;
        this.parameterTool = parameterTool;
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = MysqlConfigUtils.buildMysqlProps(parameterTool);
        queryRunner = new QueryRunner(JdbcUtils.getDataSource(properties));
    }

    @Override
    public void run(SourceContext<List<VehJobInfo>> ctx) throws Exception {
        while(true){
            List<VehJobInfo> list= queryRunner.query(sql, new BeanListHandler<>(VehJobInfo.class), params.getStartTime(), params.getEndTime());
            ctx.collect(list);
            //每隔半小时执行一次
            countDownLatch.await(5, TimeUnit.MINUTES);
        }
    }

    @Override
    public void cancel() {

    }
}
