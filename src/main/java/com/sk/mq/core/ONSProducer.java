package com.sk.mq.core;

import com.aliyun.openservices.ons.api.*;
import com.aliyun.openservices.ons.api.order.OrderProducer;
import com.sk.mq.enums.LogCategoryEnum;
import com.sk.mq.message.MQSendResult;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import com.sk.mq.support.LogThreadContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class ONSProducer implements ApplicationListener<ContextRefreshedEvent>,Admin,MQProducer {

    private Producer producer;

    private OrderProducer orderProducer;

    private Properties properties;


    private void initialize() throws Exception {
        Assert.notNull(properties,"properties not set");
        this.orderProducer = ONSFactory.createOrderProducer(properties);
        this.producer = ONSFactory.createProducer(this.properties);

        orderProducer.start();
        producer.start();
    }
    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        try {
            /**
             * 由于spring cloud 调用 feign clien时 才产生client的代理对象并且会触发ContextRefreshedEvent
             * 因此，此处判断如果已有实例化producer或orderorderProducer时，则不再调用initialize
             */
            if(isStarted()){
                return;
            }
            initialize();
        } catch (Exception e) {
            log.error("初始化ons生产者失败！", e);
        }
    }


    public void shutdown() {
        orderProducer.shutdown();
        producer.shutdown();
    }

    @Override
    public MQSendResult send(MessageBean messageBean) throws IOException {
        final com.aliyun.openservices.ons.api.Message message = SerializerFactory.onsSerialize(messageBean);
        try {
            MQSendResult mqSendResult = new MQSendResult();
            //入队前：SPRINGBOOT_MQ|topic|tags|producer|入参
            log.info("{}|{}#{}|onsProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
            SendResult sendResult = producer.send(message);
            parseResult(mqSendResult, sendResult);
            //入队后：SPRINGBOOT_MQ|topic|tags|producer|msgid|SendStatus
            log.info("{}|{}#{}|onsProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMessageId());
            return mqSendResult;
        } catch (Exception e) {
            //入队异常：SPRINGBOOT_MQ|topic|tags|producer|消息发送异常|入参
            log.error("{}|{}#{}|onsProducer|ONS消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects(), e);
            throw e;
        }//不需要ThreadContext.pop();避免某些流程后续还需要获取

    }

    private void parseResult(MQSendResult mqSendResult, SendResult sendResult) {
        if(sendResult == null || sendResult.getMessageId() == null){
            mqSendResult.setSendStatus(MQSendResult.SendStatus.SEND_FAILURE);
        }else{
            mqSendResult.setSendStatus(MQSendResult.SendStatus.SEND_OK);
            mqSendResult.setMessageId(sendResult.getMessageId());
        }
    }

    @Override
    public void sendAsync(MessageBean messageBean) throws IOException {
        final Message message = SerializerFactory.onsSerialize(messageBean);
        try {
            //入队前：SPRINGBOOT_MQ|topic#tags|producer|入参
            log.info("{}|{}#{}|onsProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
            producer.sendAsync(message, getSendCallback(messageBean));
        } catch (Exception e) {
            //入队异常：SPRINGBOOT_MQ|topic#tags|producer|消息发送异常|入参
            log.error("{}|{}#{}|onsProducer|ONS异步消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects(), e);
            throw e;
        }
    }

    private SendCallback getSendCallback(MessageBean messageBean) {
        return new SendCallback(){
            @Override
            public void onSuccess(SendResult sendResult) {
                LogThreadContext.push(messageBean.getNdc());
                log.info("{}|{}#{}|producer|异步消息发送成功|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMessageId());
                LogThreadContext.pop();
            }

            @Override
            public void onException(OnExceptionContext onExceptionContext) {
                LogThreadContext.push(messageBean.getNdc());
                log.error("{}|{}#{}|producer|异步消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), onExceptionContext.getException());
                LogThreadContext.pop();
            }
        };
    }

    @Override
    public MQSendResult sendOrderMessage(MessageBean messageBean, Object shardingKey) throws IOException {
        final com.aliyun.openservices.ons.api.Message message = SerializerFactory.onsSerialize(messageBean);
        try {
            MQSendResult mqSendResult = new MQSendResult();
            SendResult sendResult;
            if(shardingKey == null){
                //入队前：SPRINGBOOT_MQ|topic|tags|producer|入参
                log.info("{}|{}#{}|onsProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
                sendResult = producer.send(message);
                //入队后：SPRINGBOOT_MQ|topic|tags|producer|msgid|SendStatus
                log.info("{}|{}#{}|onsProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMessageId());
            }else{
                //入队前：SPRINGBOOT_MQ|topic|tags|producer|入参
                log.info("{}|{}#{}|onsOrderProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
                sendResult = orderProducer.send(message,shardingKey.toString());
                //入队后：SPRINGBOOT_MQ|topic|tags|producer|msgid|SendStatus
                log.info("{}|{}#{}|onsOrderProducer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMessageId());

            }
            parseResult(mqSendResult, sendResult);
            return mqSendResult;
        } catch (Exception e) {
            //入队异常：SPRINGBOOT_MQ|topic|tags|producer|消息发送异常|入参
            log.error("{}|{}#{}|onsOrderProducer|ONS顺序消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects(), e);
            throw e;
        }//不需要ThreadContext.pop();避免某些流程后续还需要获取

    }
    public Properties getProperties() {
        return this.properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public boolean isStarted() {
        if(producer == null || orderProducer == null){
            return false;
        }
        return this.producer.isStarted() && this.orderProducer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.producer.isClosed() && this.orderProducer.isClosed();
    }


}