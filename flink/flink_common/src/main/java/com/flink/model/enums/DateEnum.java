package com.flink.model.enums;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 9:50
 */
public enum  DateEnum {

    YEAR_MONTH_DAY("YYYY-MM-dd","年月日"),

    YEAR_MONTH_DAY_H_M_S("YYYY-MM-dd HH:mm:ss","年月日时分秒");

    DateEnum(String type, String name) {
        this.type = type;
        this.name = name;
    }

    private String type;

    private String name;

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
