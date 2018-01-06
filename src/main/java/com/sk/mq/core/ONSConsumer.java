package com.sk.mq.core;

import com.aliyun.openservices.ons.api.*;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import com.sk.mq.message.MQSendResult;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * Created by samfan on 2017-08-23.
 */
@Slf4j
public class ONSConsumer  implements ApplicationListener<ApplicationEvent>,Admin {

    private Consumer consumer;

    private OrderConsumer orderConsumer;

    private Properties properties;

    private Boolean orderlyConsume = Boolean.FALSE;

    private Map<String, ONSMessageListener> messageListeners = new HashMap<String, ONSMessageListener>();

    @Autowired
    private ONSProducer producer;

    private Set<String> topicSet;

    /**
     * 生产环境中找不到灰度环境中的handler时，重发至其它queue的时间间隔
     * 默认为3秒
     */
    private Long resendToAnotherQueueMilSeconds = 3000L;

    public Long getResendToAnotherQueueMilSeconds() {
        return resendToAnotherQueueMilSeconds;
    }

    public void setResendToAnotherQueueMilSeconds(Long resendToAnotherQueueMilSeconds) {
        this.resendToAnotherQueueMilSeconds = resendToAnotherQueueMilSeconds;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setOrderlyConsume(Boolean orderlyConsume) {
        this.orderlyConsume = orderlyConsume;
    }

    private void initialize() {
        if(CollectionUtils.isEmpty(messageListeners)){
            return;
        }
        if(orderlyConsume){
            this.orderConsumer = ONSFactory.createOrderedConsumer(properties);
        }else{
            this.consumer = ONSFactory.createConsumer(properties);
        }

        Assert.notNull(producer,"ONSConsumer 注入producer失败，无法执行发送消息");

        topicSet = resovleTopic(messageListeners);

        for (String topic:topicSet) {
            if(orderlyConsume) {
                orderConsumer.subscribe(topic, "*",(message,consumeOrderContext) -> {
                    String listenerKey = resovleListenerKey(message.getTopic(), message.getTag());
                    ONSMessageListener listener = messageListeners.get(listenerKey);
                    if(listener != null){
                        return listener.consume(message,consumeOrderContext);
                    }else {
                        try {
                            resendToAnotherQueue(message,listenerKey);
                        } catch (Exception e) {
                            log.error("消息重新发送失败..",e);
                        }
                        return OrderAction.Success ;
                    }
                });
            }else{
                consumer.subscribe(topic, "*", (message,consumeContext) -> {
                    String listenerKey = resovleListenerKey(message.getTopic(), message.getTag());
                    ONSMessageListener listener = messageListeners.get(listenerKey);
                    if(listener != null){
                        return listener.consume(message,consumeContext);
                    }else {
                        try {
                            resendToAnotherQueue(message,listenerKey);
                        } catch (Exception e) {
                            log.error("消息重新发送失败，稍候重试",e);
                            return Action.ReconsumeLater;
                        }
                        return Action.CommitMessage;
                    }
                });
            }
        }

        if(orderlyConsume) {
            orderConsumer.start();
        }else{
            consumer.start();
        }

    }

    /**
     *
     * @param messageListeners
     * @return
     */
    private Set<String> resovleTopic(Map<String, ONSMessageListener> messageListeners) {
        Set<String> topicSet = new HashSet<>();
        for (Map.Entry<String, ONSMessageListener> messageListenerEntry : messageListeners.entrySet()) {
            ONSMessageListener messageListener = messageListenerEntry.getValue();
            topicSet.add(messageListener.getTopic());
        }
        return topicSet;
    }

    public void addSubscriber(final ONSMessageListener messageListener) throws Exception {
        String key = resovleListenerKey(messageListener.getTopic(),messageListener.getTags());
        if (messageListeners.get(key) != null) {
            throw new Exception("同一topic下的同一tag不能重复订阅！");
        }
        messageListeners.put(key, messageListener);
    }

    private void resendToAnotherQueue(Message message,String key) throws Exception {
        Object object = SerializerFactory.deserialize(message);
        try {
            Thread.sleep(getResendToAnotherQueueMilSeconds());
            MQSendResult sendResult = producer.send((MessageBean)object);
            log.warn("找不到handler， 消息重发完成.tag:{}",key);
        }catch (InterruptedException e) {
            log.error("Thread InterruptedException ",e);
        }
    }

    private String resovleListenerKey(String topic,String tags){
        return topic + "#" + tags;
    }

    @Override
    public boolean isStarted() {
        if(orderlyConsume){
            if(this.orderConsumer == null){
                return false;
            }
            return this.orderConsumer.isStarted();
        }

        if(this.consumer == null){
            return false;
        }
        return this.consumer.isStarted();
    }

    @Override
    public boolean isClosed() {
        if(orderlyConsume){
           return this.orderConsumer.isClosed();
        }
        return consumer.isClosed();
    }

    //缓存class
    private Class<?> eventClazz = null;
    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        log.info("consumer listener event -->{}", event.getClass().getName());
        if (isStarted()) {
            return;
        }
        if (eventClazz == null) {
            try {
                eventClazz = Class.forName("org.springframework.boot.context.event.ApplicationReadyEvent");
            } catch (ClassNotFoundException e) {
                log.warn("current is not spring boot environment.");
            }
        }
        //spring cloud 架构
        boolean isSpringBootEnv = (eventClazz != null && event.getClass().equals(eventClazz));
        if (isSpringBootEnv) {
            initialize();
        }
        //spring3 老架构
        boolean isSpring3Env = (eventClazz == null && event instanceof ContextRefreshedEvent);
        if (isSpring3Env) {
            initialize();
        }
    }

}
