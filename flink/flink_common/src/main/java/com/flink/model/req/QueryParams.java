package com.flink.model.req;

import lombok.Data;

import java.io.Serializable;

/**
 * @author 00074964
 * @version 1.0
 * @date 2022-5-19 9:42
 */
@Data
public class QueryParams implements Serializable {
    private static final long serialVersionUID = -8557713590533875389L;

    private String startTime;

    private String endTime;

    public QueryParams(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
