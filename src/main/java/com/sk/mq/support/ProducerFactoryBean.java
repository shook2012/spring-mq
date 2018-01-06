package com.sk.mq.support;

import com.sk.mq.core.MQProducer;
import org.springframework.beans.factory.FactoryBean;

/**
 * Created by samfan on 2017-04-24.
 */
public class ProducerFactoryBean<T> implements FactoryBean<T>{

    private ProducerRegistry<T> producerRegistry;

    private Class<T> mqInterface;

    private MQProducer producer;

    public ProducerFactoryBean(Class<T> mqInterface) {
        this.mqInterface = mqInterface;
    }

    public void setProducer(MQProducer producer) {
        this.producer = producer;
    }

    public void setMqInterface(Class<T> mqInterface) {
        this.mqInterface = mqInterface;
    }

    public void setProducerRegistry(ProducerRegistry producerRegistry) {
        this.producerRegistry = producerRegistry;
    }

    public Class<T> getMqInterface() {
        return mqInterface;
    }

    @Override
    public T getObject() throws Exception {
        return producerRegistry.getProducer(mqInterface,producer);
    }

    @Override
    public Class<?> getObjectType() {
        return mqInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
