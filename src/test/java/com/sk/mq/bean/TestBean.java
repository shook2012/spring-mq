package com.sk.mq.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by samfan on 2018/8/9.
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TestBean implements Serializable{

    private Integer id;

    private String name;

    private Date time;

}
