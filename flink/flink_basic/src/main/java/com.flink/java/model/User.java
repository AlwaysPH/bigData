package com.flink.java.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Java的POJO类属性修饰符应该为public，并且必须含有无参构造方法
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
@Data
public class User implements Serializable {
    private static final long serialVersionUID = -3681841760608259641L;

    private Integer id;

    private String name;

    private Integer age;

    private Long time;

    public User() {
    }

    public User(Integer id, String name, Integer age, Long time) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.time = time;
    }
}
