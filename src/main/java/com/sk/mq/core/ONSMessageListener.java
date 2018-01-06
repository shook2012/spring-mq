package com.sk.mq.core;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.order.ConsumeOrderContext;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.sk.mq.enums.LogCategoryEnum;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import com.sk.mq.support.LogThreadContext;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;


/**
 * @author samfan
 */
@Slf4j
public class ONSMessageListener implements MessageListener,MessageOrderListener {

    private final String topic;
    private final String tags;
    private final Object bean;
    private final Method method;
    public static final String CONSUME_SUCCESS = "SUCCESS";


    public ONSMessageListener(Object bean, Method method,String topic, String tags) {
        this.bean = bean;
        this.method = method;
        this.topic = topic;
        this.tags = tags;
    }

    /**
     * 普通消费
     * @param message
     * @param consumeContext
     * @return
     */
    @Override
    public Action consume(Message message, ConsumeContext consumeContext) {
        try {
            Object object = SerializerFactory.deserialize(message);
            if (null != object && object instanceof MessageBean) {
                final MessageBean messageBean = (MessageBean) object;
                messageBean.setMsgId(message.getMsgID());
                LogThreadContext.push(messageBean.getNdc());//txnId|ip|手机号|uid
                //消费前：SPRINGBOOT_MQ|topic|{}|consumer|msgid|入参
                log.info("{}|{}|{}|ONS消息并发消费 -> onsConsumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getMsgId(), messageBean.getObjects());
                if (method.getParameterCount() == 1 && MessageBean.class.equals(method.getParameterTypes()[0])) {
                    method.invoke(bean, messageBean);
                } else {
                    method.invoke(bean, messageBean.getObjects());
                }
                //消费后：SPRINGBOOT_MQ|topic|tags|consumer|keys|ConsumeConcurrentlyStatus
                log.info("{}|{}|{}|onsConsumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), message.getKey(),CONSUME_SUCCESS);
            } else {
                method.invoke(bean, object);
            }
            return Action.CommitMessage;
        } catch (final Exception e) {
            //消费异常：SPRINGBOOT_MQ |topic|tags|consumer|消息消费异常
            log.error("{}|{}|{}|onsConsumer|ONS消息消费异常", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), message.getTopic(), message.getTag(), e);
            return Action.ReconsumeLater;
        } finally {
            LogThreadContext.pop();
        }
    }

    /**
     * 顺序消费
     * @param message
     * @param consumeOrderContext
     * @return
     */
    @Override
    public OrderAction consume(Message message, ConsumeOrderContext consumeOrderContext) {
        try {
            Object object = SerializerFactory.deserialize(message);
            if(null !=object && object instanceof MessageBean){
                final MessageBean messageBean = (MessageBean)object;
                messageBean.setMsgId(message.getMsgID());
                LogThreadContext.push(messageBean.getNdc());//txnId|ip|手机号|uid
                //消费前：SPRINGBOOT_MQ|topic|{}|onsConsumer|msgid|入参
                log.info("{}|{}|{}|ONS消息顺序消费 -> onsConsumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),messageBean.getTopic(),messageBean.getTags(),messageBean.getMsgId(),messageBean.getObjects());
                if(method.getParameterCount() == 1 && MessageBean.class.equals(method.getParameterTypes()[0])){
                    method.invoke(bean,messageBean);
                }else{
                    method.invoke(bean,messageBean.getObjects());
                }
                //消费后：SPRINGBOOT_MQ|topic|tags|onsConsumer|keys|ConsumeConcurrentlyStatus
                log.info("{}|{}|{}|onsConsumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),messageBean.getTopic(),messageBean.getTags(),message.getKey(),CONSUME_SUCCESS);
            }else{
                method.invoke(bean, object);
            }
            return OrderAction.Success;
        } catch (final Exception e) {
            //消费异常：SPRINGBOOT_MQ |topic|tags|onsConsumer|消息消费异常
            log.error("{}|{}|{}|onsConsumer|ONS消息消费异常", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),message.getTopic(),message.getTag(),e);
            return OrderAction.Success;
        }finally{
            LogThreadContext.pop();
        }
    }


    public Object getBean() {
        return bean;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getTags() {
        return tags;
    }
}
