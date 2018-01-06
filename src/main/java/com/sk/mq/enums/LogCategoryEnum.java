package com.sk.mq.enums;

/**
 * 日志类型枚举
 * 
 * @author samfan
 *
 */
public enum LogCategoryEnum {
	SPRINGBOOT_MQ("SPRINGBOOT_MQ","MQ"),
	;
	
	private String code;
	
	private String value;

	private LogCategoryEnum(String code, String value){
		
		this.code=code;
		this.value=value;
		
	}

	public String getCode() {
		return this.code;
	}

	public String getValue() {
		return this.value;
	}
	
	
	
}