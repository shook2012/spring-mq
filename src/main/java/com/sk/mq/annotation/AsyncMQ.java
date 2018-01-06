package com.sk.mq.annotation;


import com.sk.mq.enums.SerializeType;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AsyncMQ {

	//topic - 应用功能模块
	String topic();

	//tag - 对应具体业务
	String tags();

	//consumer实例
	String consumer() ;

	//是否异步发送消息
	boolean async() default false;

	//序列化方式
	SerializeType serializeType() default SerializeType.HESSIAN;
}