package com.sk.mq.core;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import com.sk.mq.message.MQSendResult;
import com.sk.mq.message.MessageBean;
import com.sk.mq.serialize.SerializerFactory;
import com.sk.mq.support.AsyncMQBootstrapConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by samfan on 2016/6/13.
 */
@Slf4j
public class RocketMQConsumer implements ApplicationListener<ApplicationEvent> {

    /**
     * nameserver地址
     */
    private String namesrvAddr;
    /**
     * topic prefix环境前缀
     */
    @Value(AsyncMQBootstrapConfiguration.ROCKETMQ_TOPIC_PREFIX_KEY)
    private String topicEnvPrefix;
    /**
     * consumer组名环境前缀，默认与topic prefix一致
     */
    private String consumerGroupPrefix;

    /**
     * consumer组名
     */
    private String consumerGroupName;

    /**
     * 消费线程池数量 -- 最小线程数
     */
    private int consumeThreadMin = 20;
    /**
     * 消费线程池数量 -- 最大线程数
     */
    private int consumeThreadMax = 64;

    /**
     * 长轮询， 拉消息间隔， 单位毫秒
     * 如果要做流控，再设置
     */
    private long pullInterval = 0;

    /**
     * 一次最多拉多少条消息
     */
    private int pullBatchSize = 32;

    /**
     * 单次消费多少条消息
     *
     * 目前RocketMQMessageListener中consumeMessage方法仅支持单次消费一条消息
     */
    private int consumeMessageBatchMaxSize = 1;

    private boolean isStart = false;
    /**
     * 消费者实例
     */
    private DefaultMQPushConsumer consumer;

    private  Map<String, RocketMQMessageListener> messageListeners = new HashMap<String, RocketMQMessageListener>();


    /**
     * 默认使用集群消费
     */
    private String messageModel;

    /**
     * 是否启用vip通道，默认关闭(否则会消费不到，这是3.5.8版本引入的)
     */
    private boolean vipChannelEnable = false;

    @Autowired
    private RocketMQProducer producer;

    private Boolean orderlyConsume = false;

    /**
     * 生产环境中找不到灰度环境中的handler时，重发至其它queue的时间间隔
     * 默认为1秒
     */
    private Long resendToAnotherQueueMilSeconds = 1000L;
    /**
     * 是否已初始化
     */

    public RocketMQConsumer() {}

    public RocketMQConsumer(final String anNamesrvAddr, final String anConsumerGroupName) {
        this.namesrvAddr = anNamesrvAddr;
        this.consumerGroupName = anConsumerGroupName;
    }

    public void initialize() throws Exception{
        Assert.notNull(namesrvAddr,"namesrvAddr 不允许为空。");
        if (CollectionUtils.isEmpty(messageListeners)){
            log.info("messageListeners is empty,init later.");
            return;
        }
        if(isStart){
            log.info("consumer:{} is started",consumer.getConsumerGroup());
            return;
        }

        consumer = new DefaultMQPushConsumer(consumerGroupName);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setVipChannelEnabled(vipChannelEnable);
        consumer.setConsumeThreadMin(consumeThreadMin);
        consumer.setConsumeThreadMax(consumeThreadMax);
        consumer.setPullBatchSize(pullBatchSize);
        consumer.setPullInterval(pullInterval);
        consumer.setMessageModel(getMessageModel());
        consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);
        /**
         * 订阅指定topic下所有消息<br>
         * 注意：一个consumer对象可以订阅多个topic
         */
        for (Map.Entry<String, RocketMQMessageListener> consumerEntry : messageListeners.entrySet()) {
            RocketMQMessageListener messageListener = consumerEntry.getValue();
            //此处订阅topic下的所有的tag  通过messageListeners的key来区分具体业务tag
            consumer.subscribe(messageListener.getTopic(), "*");
            log.info("订阅消息topic:{} ,tags:{}", messageListener.getTopic() ,messageListener.getTags());
        }

        if(this.getOrderlyConsume()){
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messageExtList, ConsumeOrderlyContext consumeOrderlyContext) {
                    MessageExt msgExt = messageExtList.get(0);
                    String key = resovleListenerKey(msgExt.getTopic(),msgExt.getTags());
                    RocketMQMessageListener listener = messageListeners.get(key);
                    if(listener != null){
                        return listener.consumeMessage(messageExtList, consumeOrderlyContext);
                    }else{
                        try {
                            //重发至其它queue
                            resendToAnotherQueue(msgExt,key);
                        } catch (Exception e) {
                            log.error("消息重新发送失败..",e);
                        }
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                }
           });
        }else{
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExtList,
                                                                ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    MessageExt msgExt = messageExtList.get(0);
                    String key = resovleListenerKey(msgExt.getTopic(),msgExt.getTags());
                    RocketMQMessageListener listener = messageListeners.get(key);
                    if(listener != null){
                        return listener.consumeMessage(messageExtList, consumeConcurrentlyContext);
                    }
                    else{
                        try {
                            //重发至其它queue
                            resendToAnotherQueue(msgExt,key);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        } catch (Exception e) {
                            log.error("consume error.message will be consume later.",e);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                }
            });
        }
        start();
    }

    public void resendToAnotherQueue(MessageExt msgExt,String key) throws Exception {
        Object object = SerializerFactory.deserialize(msgExt);
        try {
            Thread.sleep(getResendToAnotherQueueMilSeconds());
            MQSendResult sendResult = producer.send((MessageBean) object);
            log.warn("找不到handler， 消息重发完成.tag:{}",key);
        }catch (InterruptedException e) {
            log.error("Thread InterruptedException ",e);
        }
    }


    public void start() throws Exception {
        if (log.isInfoEnabled()) {
            log.debug("启动消息消费者,[ConsumerGroup]:{}" ,consumer.getConsumerGroup());
        }
        consumer.start();
        isStart = true;
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

    public Boolean getOrderlyConsume() {
        return orderlyConsume;
    }

    public void setOrderlyConsume(Boolean orderlyConsume) {
        this.orderlyConsume = orderlyConsume;
    }

    public Long getResendToAnotherQueueMilSeconds() {
        return resendToAnotherQueueMilSeconds;
    }

    public void setResendToAnotherQueueMilSeconds(Long resendToAnotherQueueMilSeconds) {
        this.resendToAnotherQueueMilSeconds = resendToAnotherQueueMilSeconds;
    }

    public void shutdown() {
        if (log.isInfoEnabled()) {
            log.debug("关闭消息消费者,[ConsumerGroup]:{}" ,consumer.getConsumerGroup());
        }
        consumer.shutdown();
    }

    public void addSubscriber(final RocketMQMessageListener messageListener) throws Exception {
        String key = resovleListenerKey(messageListener.getTopic(),messageListener.getTags());
        if (messageListeners.get(key) != null) {
            throw new Exception("同一topic下的同一tag不能重复订阅！");
        }
        messageListeners.put(key, messageListener);
    }

    public void removeSubscriber(String key) {
        RocketMQMessageListener messageListener = messageListeners.get(key);
        if (messageListener != null) {
            messageListeners.remove(key);
        }
    }

    private String resovleListenerKey(String topic,String tags){
        return topic + "#" + tags;
    }

	public void setConsumeThreadMin(int consumeThreadMin) {
		this.consumeThreadMin = consumeThreadMin;
	}

    public void setConsumerGroupPrefix(String consumerGroupPrefix) {
       this.consumerGroupPrefix = consumerGroupPrefix;
    }

	public void setConsumeThreadMax(int consumeThreadMax) {
		this.consumeThreadMax = consumeThreadMax;
	}

    public void setMessageModel(String messageModel) {
        this.messageModel = messageModel;
    }

    /**
     * 默认为集群消费
     *
     * @return
     */
    public MessageModel getMessageModel() {
        if(messageModel != null){
            for (MessageModel msgModel : MessageModel.values()) {
                if (msgModel.getModeCN().equals(messageModel)){
                    return msgModel;
                }
            }
        }
        return MessageModel.CLUSTERING;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void setVipChannelEnable(boolean vipChannelEnable) {
        this.vipChannelEnable = vipChannelEnable;
    }


    public void setConsumerGroupName(String consumerGroupName) {
        if (consumerGroupPrefix == null){
            consumerGroupPrefix = topicEnvPrefix;
        }
		this.consumerGroupName = consumerGroupPrefix + consumerGroupName;
	}

	private Class<?> eventClazz = null;

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        log.debug("consumer listener event -->{}", event.getClass().getName());
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
            init();
        }
        //spring3 架构
        boolean isSpring3Env = (eventClazz == null && event instanceof ContextRefreshedEvent);
        if (isSpring3Env) {
            init();
        }
    }

    private void init() {
        try {
            log.info("init rocketmq consumer");
            initialize();
        } catch (Exception e) {
            log.error("初始化rocketMq消费者失败！", e);
        }
    }

}
