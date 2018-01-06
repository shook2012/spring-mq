package com.sk.mq.enums;

/**
 * 
 * @author samfan
 *
 */
public enum SerializeType {
	
    HESSIAN((byte)0x01,"hessian"),KRYO((byte)0x02,"kryo"), NATIVEJAVA((byte)0x03,"nativejava");
    
    private byte type;
    
    private String name;
    
    private SerializeType(byte type, String name) {
		this.type = type;
		this.name = name;
	}
	public SerializeType getSerializeType(Object object){
    	if (object instanceof Integer) {
    		return getSerializeType((Integer)object);
    	}
    	if (object instanceof String) {
    		return getSerializeType((String)object);
    	}
    	throw new IllegalArgumentException("");
    }
    public static SerializeType getSerializeType(byte type){
    	SerializeType[] types = SerializeType.values();
    	for (SerializeType serializeType : types) {
    		if(serializeType.getType() == type){
    			return serializeType;
    		}
		}
    	return null;
    }
    public static SerializeType getSerializeType(String name){
    	SerializeType[] types = SerializeType.values();
    	for (SerializeType serializeType : types) {
    		if(name.equalsIgnoreCase(serializeType.getName())){
    			return serializeType;
    		}
		}
    	return null;
    }

	public byte getType() {
		return type;
	}
	public void setType(byte type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
