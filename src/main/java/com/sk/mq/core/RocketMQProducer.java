package com.sk.mq.core;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import com.sk.mq.enums.LogCategoryEnum;
import com.sk.mq.message.MQSendResult;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import com.sk.mq.support.LogThreadContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.StringUtils;

import java.util.List;

@Slf4j
public class RocketMQProducer implements ApplicationListener<ContextRefreshedEvent>,MQProducer{

    /**
     * nameserver地址
     */
    private String namesrvAddr;
    /**
     * producer组名
     */
    private String producerGroupName;

    /**
     * 发送消息，自动创建不存在的topic队列数,默认4
     */
    private int defaultTopicQueueNums = 4;

    /**
     * 发送消息超时时间 单位毫秒
     */
    private int sendMsgTimeout = 10000;
    /**
     * 发送消息失败重试次数
     */
    private int retryTimesWhenSendFailed = 1;

    private boolean isStart = false;

    /**
     * 是否启用vip通道，默认关闭
     */
    private boolean vipChannelEnable = false;

    private DefaultMQProducer producer;

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void setProducerGroupName(String producerGroupName) {
        this.producerGroupName = producerGroupName;
    }

    public boolean isVipChannelEnable() {
        return vipChannelEnable;
    }

    public void setVipChannelEnable(boolean vipChannelEnable) {
        this.vipChannelEnable = vipChannelEnable;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public RocketMQProducer() {
    }

    public RocketMQProducer(final String anNamesrvAddr, final String anProducerGroupName) {
        this.namesrvAddr = anNamesrvAddr;
        this.producerGroupName = anProducerGroupName;
    }

    private void initialize() throws Exception {
        if (StringUtils.isEmpty(namesrvAddr)) {
            throw new Exception("namesrvAddr 不允许为空。");
        }

        producer = new DefaultMQProducer(producerGroupName);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);
        producer.setVipChannelEnabled(isVipChannelEnable());
        producer.setSendMsgTimeout(sendMsgTimeout);
        producer.setDefaultTopicQueueNums(defaultTopicQueueNums);//topic 默认4个queue，避免开发环境多个订阅造成消费者收不到消息问题
        start();
    }


    /**
     * 发送同步消息
     * @param messageBean
     * @return
     * @throws Exception
     */
    @Override
    public MQSendResult send(MessageBean messageBean) throws Exception {
        return sendMessage(messageBean,null);
    }

    @Override
    public void sendAsync(MessageBean messageBean) throws Exception {
        final Message message = SerializerFactory.serialize(messageBean);
        try {
            //入队前：SPRINGBOOT_MQ|topic#tags|producer|入参
            log.info("{}|{}#{}|producer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
            producer.send(message, getSendCallback(messageBean));

        } catch (Exception e) {
            //入队异常：SPRINGBOOT_MQ|topic#tags|producer|消息发送异常|入参
            log.error("{}|{}#{}|producer|消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects(), e);
            throw e;
        }
    }

    @Override
    public MQSendResult sendOrderMessage(MessageBean messageBean, Object shardingKey) throws Exception {
        return sendMessage(messageBean,shardingKey);
    }



    /**
     * 发送同步消息
     * @param messageBean
     * @param shardingKey 分queue键
     * @return
     * @throws Exception
     */
    public MQSendResult sendMessage(final MessageBean messageBean, Object shardingKey) throws Exception {
        final Message message = SerializerFactory.serialize(messageBean);
        try {
            MQSendResult mqSendResult = new MQSendResult();
            //入队前：SPRINGBOOT_MQ|topic|tags|producer|入参
            log.info("{}|{}#{}|producer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects());
            SendResult sendResult;
            if(shardingKey == null){
                sendResult = producer.send(message);
            }else {
                sendResult = producer.send(message,getMessageQueueSelector(),shardingKey);
            }
            //入队后：SPRINGBOOT_MQ|topic|tags|producer|msgid|SendStatus
            log.info("{}|{}#{}|producer|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMsgId());
            parseResult(mqSendResult,sendResult);
            return mqSendResult;
        } catch (Exception e) {
            //入队异常：SPRINGBOOT_MQ|topic|tags|producer|消息发送异常|入参
            log.error("{}|{}#{}|producer|消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), messageBean.getObjects(), e);
            throw e;
        }//不需要ThreadContext.pop();避免某些流程后续还需要获取
    }


    private MessageQueueSelector getMessageQueueSelector() {
        return new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object partQueueKey) {
                int queueId = 0;
                int queueSize = list.size();
                if (partQueueKey instanceof Number){
                    int key = ((Number)partQueueKey).intValue();
                    queueId = key % queueSize;
                }else{
                    int hashCode = partQueueKey.hashCode();
                    if(hashCode < 0) {
                        hashCode = Math.abs(hashCode);
                    }

                    queueId = hashCode % queueSize;
                }
                return list.get(queueId);
            }
        };
    }

    /**
     * 异步消息callback方法
     * @param messageBean
     * @return
     */
    private SendCallback getSendCallback(final MessageBean messageBean) {
        return new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                LogThreadContext.push(messageBean.getNdc());
                log.info("{}|{}#{}|producer|异步消息发送成功|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), sendResult.getMsgId());
                LogThreadContext.pop();
            }

            @Override
            public void onException(Throwable throwable) {
                LogThreadContext.push(messageBean.getNdc());
                log.error("{}|{}#{}|producer|异步消息发送异常|{}", LogCategoryEnum.SPRINGBOOT_MQ.getCode(), messageBean.getTopic(), messageBean.getTags(), throwable);
                LogThreadContext.pop();
            }
        };
    }

    private void parseResult(MQSendResult mqSendResult, SendResult sendResult) {
        if(sendResult == null || sendResult.getMsgId() == null){
            mqSendResult.setSendStatus(MQSendResult.SendStatus.SEND_FAILURE);
        }else{
            mqSendResult.setSendStatus(MQSendResult.SendStatus.SEND_OK);
            mqSendResult.setMessageId(sendResult.getMsgId());
        }
    }


    private void start() throws MQClientException {

        log.info("启动消息生产者|producerGroup={}", producer.getProducerGroup());

        producer.start();
        shutdownHook();
    }

    public void shutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdown();
            }
        }));
    }

    /**
     * 关闭服务
     */
    public void shutdown() {

        log.info("关闭消息生产者|producerGroup={}", producer.getProducerGroup());

        producer.shutdown();
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent arg0) {
        if (isStart) {
            return;
        }
        isStart = true;

        try {
            initialize();
        } catch (Exception e) {
            log.error("初始化rocketMq生产者失败！", e);
        }
    }

}
