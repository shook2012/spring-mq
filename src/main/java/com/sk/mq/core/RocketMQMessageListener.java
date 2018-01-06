package com.sk.mq.core;

import com.alibaba.rocketmq.client.consumer.listener.*;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.sk.mq.config.MethodRocketMQListenerEndpoint;
import com.sk.mq.enums.LogCategoryEnum;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import com.sk.mq.support.LogThreadContext;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.List;


/**
 * @author samfan
 */
@Slf4j
public class RocketMQMessageListener implements MessageListenerConcurrently,MessageListenerOrderly {

    private final String topic;
    private final String tags;
    private final Object bean;
    private final Method method;

    /**
     * @param endpoint
     */
    public RocketMQMessageListener(final MethodRocketMQListenerEndpoint endpoint) {
        this.bean = endpoint.getBean();
        this.method = endpoint.getMethod();
        this.topic = endpoint.getTopic();
        this.tags = endpoint.getTags();
    }

    /**
     * @param messageExtList
     * @param context
     * @return
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExtList, ConsumeConcurrentlyContext context) {
        MessageExt messageExt = messageExtList.get(0);
        try {
            Object object = SerializerFactory.deserialize(messageExt);
            if(null !=object && object instanceof MessageBean){
                final MessageBean message = (MessageBean)object;
                message.setMsgId(messageExt.getMsgId());
                LogThreadContext.push(message.getNdc());//txnId|ip|手机号|uid
                //消费前：SPRINGBOOT_MQ|topic|{}|consumer|msgid|入参
                log.info("{}|{}|{}|消息并发消费 -> consumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),message.getTopic(),message.getTags(),message.getMsgId(),message.getObjects());
                if(method.getParameterCount() == 1 && MessageBean.class.equals(method.getParameterTypes()[0])){
                    method.invoke(bean,message);
                }else{
                    method.invoke(bean,message.getObjects());
                }
                //消费后：SPRINGBOOT_MQ|topic|tags|consumer|keys|ConsumeConcurrentlyStatus
                log.info("{}|{}|{}|consumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),message.getTopic(),message.getTags(),messageExt.getKeys(),ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
            }else{
            	method.invoke(bean, object);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (final Exception e) {
            //消费异常：SPRINGBOOT_MQ |topic|tags|consumer|消息消费异常
            log.error("{}|{}|{}|consumer|消息消费异常", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),messageExt.getTopic(),messageExt.getTags(),e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }finally{
        	LogThreadContext.pop();
        }
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messageExtList, ConsumeOrderlyContext consumeOrderlyContext) {
        MessageExt messageExt = messageExtList.get(0);
        try {
            Object object = SerializerFactory.deserialize(messageExt);
            if(null !=object && object instanceof MessageBean){
                final MessageBean message = (MessageBean)object;
                message.setMsgId(messageExt.getMsgId());
                LogThreadContext.push(message.getNdc());//txnId|ip|手机号|uid
                //消费前：SPRINGBOOT_MQ|topic|{}|consumer|msgid|入参
                log.info("{}|{}|{}|消息顺序消费 -> consumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),message.getTopic(),message.getTags(),message.getMsgId(),message.getObjects());
                if(method.getParameterCount() == 1 && MessageBean.class.equals(method.getParameterTypes()[0])){
                    method.invoke(bean,message);
                }else{
                    method.invoke(bean,message.getObjects());
                }
                //消费后：SPRINGBOOT_MQ|topic|tags|consumer|keys|ConsumeConcurrentlyStatus
                log.info("{}|{}|{}|consumer|{}|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),message.getTopic(),message.getTags(),messageExt.getKeys(),ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
            }else{
                method.invoke(bean, object);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        } catch (final Exception e) {
            //消费异常：SPRINGBOOT_MQ |topic|tags|consumer|消息消费异常
            log.error("{}|{}|{}|consumer|消息消费异常", LogCategoryEnum.SPRINGBOOT_MQ.getCode(),messageExt.getTopic(),messageExt.getTags(),e);
            return ConsumeOrderlyStatus.SUCCESS;
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
