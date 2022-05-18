package com.flink.model;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-18 14:25
 */
@Data
public class VehJobInfo implements Serializable {
    private static final long serialVersionUID = -5130614360466966755L;

    /**
     * 主键id
     */
    private Long id;

    /**
     *标段ID
     */
    private Integer projectId;

    /**
     *车辆编号
     */
    private Integer vehId;

    /**
     *车辆运行日期
     */
    private String workDate;

    /**
     *行驶总油耗量
     */
    private BigDecimal todayTotalFuel;

    /**
     *作业总油耗量
     */
    private BigDecimal todayWorkFuel;

    /**
     *当天总行驶里程
     */
    private BigDecimal todayTotalMileage;

    /**
     *当天作业里程
     */
    private BigDecimal todayWorkMileage;

    /**
     *作业总时长
     */
    private BigDecimal todayWorkTimes;

    /**
     *行驶时长
     */
    private BigDecimal todayTotalTimes;

    /**
     *当天开始油耗量
     */
    private BigDecimal firstFuel;

    /**
     *当天最后油耗量
     */
    private BigDecimal lastFuel;

    /**
     *当天运行开始时间
     */
    private String firstGpsTime;

    /**
     *当天最后运行时间
     */
    private Date lastGpsTime;

    /**
     *当天开始里程数
     */
    private BigDecimal firstMileage;

    /**
     *当天最后里程数
     */
    private BigDecimal lastMileage;

    /**
     *加水次数
     */
    private Integer addWaterCount;

    /**
     *收集点数(每次收集点次数和)/转运中转站数(非填埋场作业次数)
     */
    private Integer collectNumber;

    /**
     *平均加水作业里程：作业里程/加水次数
     */
    private BigDecimal addWaterAvgMileage;

    /**
     *作业面积
     */
    private BigDecimal workArea;

    /**
     *收集垃圾量(车型净载重*推铲卸料次数)，单位：千克
     */
    private BigDecimal totalCollect;

    /**
     *转运趟次(填埋场卸料次数)
     */
    private Integer transportNum;

    /**
     *在线时长，单位：分钟
     */
    private BigDecimal onlineTimes;

    /**
     *车辆类型，清扫-1，清运-2，其他-3
     */
    private Integer vehType;

    /**
     *车牌号
     */
    private String vbiLicense;

    /**
     *标段
     */
    private String projectName;

    /**
     *车辆大类
     */
    private String vehClassName;

    /**
     *车辆小类
     */
    private String vehSecondClassName;

}
