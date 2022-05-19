package com.flink.model.enums;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 9:50
 */
public enum  DateEnum {

    YEAR_MONTH_DAY("YYYY-MM-dd","年月日");

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
