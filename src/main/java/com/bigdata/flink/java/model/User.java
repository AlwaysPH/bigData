package com.bigdata.flink.java.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
@Data
public class User implements Serializable {
    private static final long serialVersionUID = -3681841760608259641L;

    private Integer id;

    private String name;

    private Integer age;

    public User(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }
}
