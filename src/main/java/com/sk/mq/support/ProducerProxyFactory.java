package com.sk.mq.support;

import com.sk.mq.core.MQProducer;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by samfan on 2017-04-21.
 */
public class ProducerProxyFactory<T> {

    private final Class<T> mqInterface;
    private Map<Method, String> methodCache = new ConcurrentHashMap<Method, String>();

    public ProducerProxyFactory(Class<T> mapperInterface) {
        this.mqInterface = mapperInterface;
    }

    public Class<T> getMqInterface() {
        return mqInterface;
    }

    public Map<Method, String> getMethodCache() {
        return methodCache;
    }

    @SuppressWarnings("unchecked")
    protected T newInstance(ProducerProxy producerProxy) {
        return (T) Proxy.newProxyInstance(mqInterface.getClassLoader(), new Class[] {mqInterface}, producerProxy);
    }

    public T newInstance(MQProducer producer, ConfigurableBeanFactory beanFactory) {
        final ProducerProxy producerProxy = new ProducerProxy(producer,mqInterface,methodCache);
        producerProxy.setBeanFactory(beanFactory);
        return newInstance(producerProxy);
    }

}
