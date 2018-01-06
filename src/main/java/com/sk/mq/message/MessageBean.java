package com.sk.mq.message;


import com.sk.mq.enums.SerializeType;

import java.io.Serializable;

/**
 * Created by samfan on 2017-04-26
 * 不建议外部接口使用MessageBean作为参数
 */
public class MessageBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;

    private Integer delayLevel;

    private String tags;

    private SerializeType serializeType;

    private Object[] objects;

    private String msgId;
    //NDC上下文
    private String ndc;

    /**
     * 分队键
     */
    private Object partQueueKey;

    public MessageBean() {
    }

    public MessageBean(String topic) {
        this.topic = topic;
    }

    public MessageBean(String topic, String tags) {
        this.topic = topic;
        this.tags = tags;
    }

    public MessageBean(final String topic, SerializeType serializeType) {
        this.topic = topic;
        this.serializeType = serializeType;
    }

    public MessageBean(String topic, SerializeType serializeType, String tags) {
        this(topic, serializeType);
        this.tags = tags;
    }

    public MessageBean(String topic, Object... objects) {
        this(topic);
        this.objects = objects;
    }

    public MessageBean(final String topic, final String tags, final Object... objects) {
        this(topic, objects);
        this.tags = tags;
    }

    public MessageBean(String topic, SerializeType serializeType, Object... objects) {
        this(topic, serializeType);
        this.objects = objects;
    }

    public MessageBean(String topic, SerializeType serializeType, String tags, Object... objects) {
        this(topic, serializeType, tags);
        this.objects = objects;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tag) {
        this.tags = tag;
    }

    public Object[] getObjects() {
        return objects;
    }

    public void setObjects(Object... objects) {
        this.objects = objects;
    }

    public SerializeType getSerializeType() {
        return serializeType;
    }

    public void setSerializeType(SerializeType serializeType) {
        this.serializeType = serializeType;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getNdc() {
        return ndc;
    }

    //新增NDC上下文,统一在ProducerProxy中处理
    public void setNdc(String ndc) {
        this.ndc = ndc;
    }

    public Object getPartQueueKey() {
        return partQueueKey;
    }

    public void setPartQueueKey(Object partQueueKey) {
        this.partQueueKey = partQueueKey;
    }
}
