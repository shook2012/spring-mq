package com.sk.mq.serialize;

import com.alibaba.rocketmq.common.message.Message;
import com.sk.mq.codec.ObjectInput;
import com.sk.mq.codec.ObjectOutput;
import com.sk.mq.codec.Serializer;
import com.sk.mq.codec.support.hessian.Hessian2Serialization;
import com.sk.mq.codec.support.nativejava.NativeJavaSerialization;
import com.sk.mq.enums.SerializeType;
import com.sk.mq.message.MessageBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * 
 * @author samfan
 *
 */
public class SerializerFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(SerializerFactory.class);
	
	private static String defaultSerializeType;
	
    public static void setDefaultSerializeType(String defaultSerializeType) {
		SerializerFactory.defaultSerializeType = defaultSerializeType;
	}

    private final static Serializer HESSIAN_SERIALIZER = new Hessian2Serialization();

    private final static Serializer NATIVEJAVA_SERIALIZER = new NativeJavaSerialization();

	//private final static Serializer KRYO_SERIALIZER = new KryoSerialization();

    private static Serializer getSerializer(SerializeType serializeType) {
    	switch (serializeType) {
    	case HESSIAN:
    		return HESSIAN_SERIALIZER;
    	/*case KRYO:
    		return KRYO_SERIALIZER;*/
    	case NATIVEJAVA:
    		return NATIVEJAVA_SERIALIZER;
    	default:
    		return HESSIAN_SERIALIZER;
    	}
    }
    
    public final static Serializer getSerializer() {
    	Assert.notNull(defaultSerializeType, "defaultSerializeType must be not null");
       SerializeType serializeType= SerializeType.getSerializeType(defaultSerializeType);
       return getSerializer(serializeType);
    }
    
    
    /**
	 * @param messageBean
	 * @return
	 * @throws IOException
	 */
	public final static Message serialize(final MessageBean messageBean) throws IOException {
		SerializeType serializeType = messageBean.getSerializeType();
		if(null == serializeType){
			serializeType = SerializeType.getSerializeType(defaultSerializeType);
		}
		try  {
			final Serializer serializer = getSerializer(serializeType);
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			final ObjectOutput objectOutput = serializer.serialize(output);
			objectOutput.writeObject(messageBean);
			objectOutput.flushBuffer();
			byte[] body = output.toByteArray();
			byte[] buf = new byte[body.length + 1];
			buf[0] = serializeType.getType();
			System.arraycopy(body, 0, buf, 1, body.length);
			final Message message = new Message(messageBean.getTopic(),messageBean.getTags(),buf);
			Integer delayLevel = messageBean.getDelayLevel();
			if(delayLevel != null){
				message.setDelayTimeLevel(delayLevel);
			}
			return message;
		}catch(Exception e){
			logger.error("serialize error",e);
			return null;
		}
	}

	/**
	 * @param messageBean
	 * @return
	 * @throws IOException
	 */
	public final static com.aliyun.openservices.ons.api.Message onsSerialize(final MessageBean messageBean) throws IOException {
		SerializeType serializeType = messageBean.getSerializeType();
		if(null == serializeType){
			serializeType = SerializeType.getSerializeType(defaultSerializeType);
		}
		try  {
			final Serializer serializer = getSerializer(serializeType);
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			final ObjectOutput objectOutput = serializer.serialize(output);
			objectOutput.writeObject(messageBean);
			objectOutput.flushBuffer();
			byte[] body = output.toByteArray();
			byte[] buf = new byte[body.length + 1];
			buf[0] = serializeType.getType();
			System.arraycopy(body, 0, buf, 1, body.length);
			final com.aliyun.openservices.ons.api.Message message = new com.aliyun.openservices.ons.api.Message(messageBean.getTopic(),messageBean.getTags(),buf);
			return message;
		}catch(Exception e){
			logger.error("serialize error",e);
			return null;
		}
	}

    /**
     * @param message
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
	public final static Object deserialize(final Message message){
    	Assert.notNull(message, "message must be not null");
        final byte[] body = message.getBody();
        final byte[] buf = new byte[body.length - 1];
        System.arraycopy(body, 1, buf, 0, body.length - 1);
        byte serializeType = body[0];
        Serializer serializer = SerializerFactory.getSerializer(SerializeType.getSerializeType(serializeType));
        ObjectInput objectInput;
		try {
			objectInput = serializer.deserialize(new ByteArrayInputStream(buf));
			return objectInput.readObject();
		} catch (Exception e) {
			logger.error("deserialize error",e);
            return null;
		}
    }

    /**
     * @param message
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
	public final static Object deserialize(final com.aliyun.openservices.ons.api.Message message){
    	Assert.notNull(message, "message must be not null");
        final byte[] body = message.getBody();
        final byte[] buf = new byte[body.length - 1];
        System.arraycopy(body, 1, buf, 0, body.length - 1);
        byte serializeType = body[0];
        Serializer serializer = SerializerFactory.getSerializer(SerializeType.getSerializeType(serializeType));
        ObjectInput objectInput;
		try {
			objectInput = serializer.deserialize(new ByteArrayInputStream(buf));
			return objectInput.readObject();
		} catch (Exception e) {
			logger.error("deserialize error",e);
            return null;
		}
    }
	
	public static void main(String[] args) throws Exception {
		long begin = System.currentTimeMillis();
		/*for (int i = 0; i < 100000; i++) {
			Message message = serialize(new MQMessage("test_serialize_performance",SerializeType.FST,new HashMap<String,Object>(){{put("intKey",1);put("StringKey","阿斯顿马丁");}}));
			deserialize(message);
		}*/
		long end = System.currentTimeMillis();
		
		/*begin = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			Message message = serialize(new MQMessage("test_serialize_performance",SerializeType.HESSIAN,new HashMap<String,Object>(){{put("intKey",1);put("StringKey","阿斯顿马丁");}}));
			deserialize(message);
		}
		end = System.currentTimeMillis();
		System.out.println("hessian  serialize type cost seconds:" + (end - begin)/1000);*/
		/*TestBean testBean = new TestBean();
		testBean.setId(1);
		testBean.setName("hello");
		testBean.setAmount(BigDecimal.valueOf(123312313245645.22));*/
		/*
		begin = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			Message message = serialize(new MessageBean("test_serialize_performance",SerializeType.KRYO,new HashMap<String,Object>(){{put("intKey",1);put("StringKey","阿斯顿马丁");}}));
			deserialize(message);
		}
		end = System.currentTimeMillis();
		System.out.println("kryo  serialize type cost seconds:" + (end - begin)/1000);*/
		
		/*begin = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			Message message = serialize(new MessageBean("test_serialize_performance",SerializeType.HESSIAN,testBean));
			MessageBean a = (MessageBean)deserialize(message);
			System.out.println(((TestBean)a.getObjects()[0]).getAmount());
		}
		end = System.currentTimeMillis();
		System.out.println("nativejava  serialize type cost seconds:" + (end - begin)/1000);*/
	}
}
