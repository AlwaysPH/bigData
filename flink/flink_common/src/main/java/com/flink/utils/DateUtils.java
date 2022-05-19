package com.flink.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 9:46
 */
public class DateUtils {

    private static final String YEAR_MONTH_DAY = "YYYY-MM-dd";

    /**
     * 时间转字符串
     * @param date
     * @param format
     * @return
     */
    public static String parseToString(Date date, String format){
        SimpleDateFormat sd = new SimpleDateFormat(format);
        return sd.format(date);
    }
}
