package com.sk.mq.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * 消息延时级别枚举,对应rocketmq消息延时级别
 * 
 * @author samfan
 */
public enum DelayLevelEnum {
	NO_DELAY(0,0,"不延时消息"),
	LEVEL_1(1,1,"延时1s"),
	LEVEL_2(2,5,"延时5s"),
	LEVEL_3(3,10,"延时10s"),
	LEVEL_4(4,30,"延时30s"),
	LEVEL_5(5,60,"延时1m"),
	LEVEL_6(6,2*60,"延时2m"),
	LEVEL_7(7,3*60,"延时3m"),
	LEVEL_8(8,4*60,"延时4m"),
	LEVEL_9(9,5*60,"延时5m"),
	LEVEL_10(10,6*60,"延时6m"),
	LEVEL_11(11,7*60,"延时7m"),
	LEVEL_12(12,8*60,"延时8m"),
	LEVEL_13(13,9*60,"延时9m"),
	LEVEL_14(14,10*60,"延时10m"),
	LEVEL_15(15,20*60,"延时20m"),
	LEVEL_16(16,30*60,"延时30m"),
	LEVEL_17(17,60*60,"延时1h"),
	LEVEL_18(18,2*60*60,"延时2h");
	
	private DelayLevelEnum(int level, int seconds, String desc) {
		this.level = level;
		this.seconds = seconds;
		this.desc = desc;
	}
	
	private int level;
	private int seconds;
	private String desc;
	private static final Map<Integer, DelayLevelEnum> SECONDS_MAP = new HashMap<Integer, DelayLevelEnum>();

	static{
		for(DelayLevelEnum delayLevelEnum : DelayLevelEnum.values()){
			SECONDS_MAP.put(delayLevelEnum.seconds, delayLevelEnum);
		}
	}
	
	public static DelayLevelEnum getDelayLevelEnum(int seconds) {
		return SECONDS_MAP.get(seconds);
	}
	
	public int getLevel() {
		return level;
	}

	public String getDesc() {
		return desc;
	}

	public int getSeconds() {
		return seconds;
	}

}
