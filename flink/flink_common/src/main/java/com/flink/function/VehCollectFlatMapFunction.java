package com.flink.function;

import com.flink.model.VehJobInfo;
import com.flink.model.enums.DateEnum;
import com.flink.utils.DateUtils;
import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 自定义垃圾收运数据聚合
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 14:31
 */
@Log4j2
public class VehCollectFlatMapFunction extends RichFlatMapFunction<List<VehJobInfo>, List<VehJobInfo>> {

    //垃圾总量
    private MapState<String, Long> collectState;

    //转运次数
    private MapState<String, Integer> transState;

    //在线时长
    private MapState<String, Double> onlineTimeState;

    //行驶里程
    private MapState<String, Double> totalMileState;

    //行驶时长
    private MapState<String, Double> totalTimeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //设置状态TTL
        StateTtlConfig stateTtlConfig = StateTtlConfig
                // 状态有效时间    1天过期
                .newBuilder(Time.days(1))
                // 设置状态的更新类型
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 已过期还未被清理掉的状态数据不返回
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // 过期对象的清理策略 全量清理
                .cleanupFullSnapshot()
                .build();
        // MapState 状态管理配置
        MapStateDescriptor collectDes = new MapStateDescriptor("collectState", String.class, Long.class);
        MapStateDescriptor transDes = new MapStateDescriptor("transState", String.class, Integer.class);
        MapStateDescriptor onlineDes = new MapStateDescriptor("onlineTimeState", String.class, Double.class);
        MapStateDescriptor mileDes = new MapStateDescriptor("totalMileState", String.class, Double.class);
        MapStateDescriptor totalTimeDes = new MapStateDescriptor("totalTimeState", String.class, Double.class);

        // 启用状态的存活时间
        collectDes.enableTimeToLive(stateTtlConfig);
        transDes.enableTimeToLive(stateTtlConfig);
        onlineDes.enableTimeToLive(stateTtlConfig);
        mileDes.enableTimeToLive(stateTtlConfig);
        totalTimeDes.enableTimeToLive(stateTtlConfig);

        //获取状态
        collectState = getRuntimeContext().getMapState(collectDes);
        transState = getRuntimeContext().getMapState(transDes);
        onlineTimeState = getRuntimeContext().getMapState(onlineDes);
        totalMileState = getRuntimeContext().getMapState(mileDes);
        totalTimeState = getRuntimeContext().getMapState(totalTimeDes);
    }

    @Override
    public void flatMap(List<VehJobInfo> vehJobInfos, Collector<List<VehJobInfo>> collector) throws Exception {
        Map<String, List<VehJobInfo>> map = vehJobInfos.stream().collect(Collectors.groupingBy(e -> {
            return e.getVehId() + "~" + e.getWorkDate();
        }));
        List<VehJobInfo> result = Lists.newArrayList();
        map.forEach((key, tList) -> {
            Long collectSum = tList.stream().mapToLong(data -> null == data.getTotalCollect() ? 0L : data.getTotalCollect().longValue()).sum();
            Integer transSum = tList.stream().mapToInt(VehJobInfo::getTransportNum).sum();
            Double onlineSum = tList.stream().mapToDouble(data -> null == data.getOnlineTimes() ? 0.00D : data.getOnlineTimes().doubleValue()).sum();
            Double mileSum = tList.stream().mapToDouble(data -> null == data.getTodayTotalMileage() ? 0.00D : data.getTodayTotalMileage().doubleValue()).sum();
            Double totalTimeSum = tList.stream().mapToDouble(data -> null == data.getTodayTotalTimes() ? 0.00D : data.getTodayTotalTimes().doubleValue()).sum();
            try {
                Long collect = collectState.get(key) == null ? 0L : collectState.get(key);
                Integer trans = transState.get(key) == null ? 0 : transState.get(key);
                Double online = onlineTimeState.get(key) == null ? 0.0D : onlineTimeState.get(key);
                Double mile = totalMileState.get(key) == null ? 0.00D : totalMileState.get(key);
                Double time = totalTimeState.get(key) == null ? 0.00D : totalTimeState.get(key);
                VehJobInfo info = tList.get(0);
                info.setTodayTotalMileage(new BigDecimal(mileSum + mile).setScale(2, BigDecimal.ROUND_HALF_UP));
                info.setTodayTotalTimes(new BigDecimal(totalTimeSum + time).setScale(2, BigDecimal.ROUND_HALF_UP));
                info.setTotalCollect(new BigDecimal(collectSum + collect).setScale(2, BigDecimal.ROUND_HALF_UP));
                info.setTransportNum(transSum + trans);
                info.setOnlineTimes(new BigDecimal(onlineSum + online).setScale(2, BigDecimal.ROUND_HALF_UP));
                info.setUpdateTime(DateUtils.parseToString(new Date(), DateEnum.YEAR_MONTH_DAY_H_M_S.getType()));
                result.add(info);
                collectState.put(key, collectSum + collect);
                transState.put(key, transSum + trans);
                onlineTimeState.put(key, onlineSum + online);
                totalMileState.put(key, mileSum + mile);
                totalTimeState.put(key, totalTimeSum + time);
            } catch (Exception e) {
                log.error("垃圾收运数据聚合失败", e);
            }
        });
        collector.collect(result);
    }
}
