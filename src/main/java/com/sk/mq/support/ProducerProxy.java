package com.sk.mq.support;

import com.sk.mq.annotation.AsyncMQ;
import com.sk.mq.annotation.DelaySecond;
import com.sk.mq.annotation.ShardingKey;
import com.sk.mq.core.MQProducer;
import com.sk.mq.core.RocketMQProducer;
import com.sk.mq.message.MessageBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

import static com.sk.mq.ons.AsyncONSBootstrapConfiguration.ONS_TOPIC_PREFIX_KEY;
import static com.sk.mq.support.AsyncMQBootstrapConfiguration.ROCKETMQ_TOPIC_PREFIX_KEY;

/**
 * Created by samfan on 2017-04-21.
 */
@Slf4j
public class ProducerProxy<T> implements InvocationHandler,Serializable {

    private MQProducer producer;

    private final Class<T> mqInterface;

    private final Map<Method, String> methodCache;

    private ConfigurableBeanFactory beanFactory;


    public ProducerProxy(MQProducer producer,Class<T> mqInterface,Map<Method, String> methodCache) {
        this.producer = producer;
        this.methodCache = methodCache;
        this.mqInterface = mqInterface;
    }

    /**
     */
    public void setBeanFactory(ConfigurableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] paramObjects) throws Throwable {
        try{
            if(Object.class.equals(method.getDeclaringClass())){
                return method.invoke(this,paramObjects);
            }
            Assert.notEmpty(paramObjects, "unsuppport method:" + method.getName() + ",args must not empty");

            for (Object paramObject : paramObjects) {
                if (null != paramObject) {
                    Assert.isInstanceOf(Serializable.class, paramObject, "the argument must be serializable");
                }
            }

            AsyncMQ annotation = AnnotationUtils.findAnnotation(method,AsyncMQ.class);

            String prefixKey = producer instanceof RocketMQProducer ?
                               ROCKETMQ_TOPIC_PREFIX_KEY : ONS_TOPIC_PREFIX_KEY;
            String topicPrefix = resolveExpression(prefixKey);
            if(topicPrefix.equals(prefixKey)){
                topicPrefix = "";
            }
            String topic = topicPrefix + resolveExpression(annotation.topic());
            String tags = resolveExpression(annotation.tags());

            MessageBean messageBean = null;
            //接口参数允许为MessageBean,如果为MessageBean，那么只允许有一个参数
            // 不建议使用MessageBean作为参数
            if (paramObjects.length == 1 && paramObjects[0] instanceof  MessageBean){
                messageBean = (MessageBean)paramObjects[0];
                messageBean.setTopic(topic);
                messageBean.setTags(tags);
                messageBean.setSerializeType(annotation.serializeType());
            }else{
                messageBean = new MessageBean(topic,annotation.serializeType(),tags,paramObjects);
            }

            int shardingKeyIndex = findParameterAnnotation(method,ShardingKey.class);
            if(shardingKeyIndex != -1){
                messageBean.setPartQueueKey(paramObjects[shardingKeyIndex]);
            }

            int delayLevelIndex = findParameterAnnotation(method,DelaySecond.class);
            if(delayLevelIndex != -1){
                Integer delayLevel = (Integer)paramObjects[delayLevelIndex];
                messageBean.setDelayLevel(delayLevel);
            }

            messageBean.setNdc(LogThreadContext.peek());
            boolean async = annotation.async();
            //分queue键
            Object partQueueKey = messageBean.getPartQueueKey();
            if(async && partQueueKey == null){
                producer.sendAsync(messageBean);
            }else{
                return producer.sendOrderMessage(messageBean,partQueueKey);
            }

        }catch (Exception e){
            log.error("消息发送失败",e);
        }
        return null;
    }

    /**
     * 获取注解topic及tags的值，如果properties 中没有此key，则返回key本身
     * 因此@AsyncMQ注解中的topic及tags，也可以直接使用字符串常量
     * @param expression
     * @return
     */
    private String resolveExpression(String expression){
        return beanFactory.resolveEmbeddedValue(expression);
    }

    /**
     *
     * @param method
     * @return
     */
    private int findParameterAnnotation(Method method,Class clazz){
        for (int i = 0; i < method.getParameterCount() ; i++) {
            MethodParameter methodParameter = new MethodParameter(method,i);
            Annotation annotation = methodParameter.getParameterAnnotation(clazz);
            if(null != annotation){
                return i;
            }
        }
        return -1;
    }

}
