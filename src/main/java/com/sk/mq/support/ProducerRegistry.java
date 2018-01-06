package com.sk.mq.support;

import com.sk.mq.core.MQProducer;
import com.sk.mq.exceptions.BindingException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by samfan on 2017-04-24.
 */
public class ProducerRegistry<T> {

    private ConfigurableBeanFactory beanFactory;
    private final Map<Class<?>,ProducerProxyFactory<T>> producerMappers = new HashMap<Class<?>, ProducerProxyFactory<T>>();

    public void setBeanFactory(ConfigurableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    public <T> T getProducer(Class<T> type, MQProducer producer){
        final ProducerProxyFactory<T> producerProxyFactory = (ProducerProxyFactory<T>)producerMappers.get(type);
        if(producerProxyFactory == null){
            throw new BindingException("Type " + type + " is not known the ProducerRegistry");
        }
        try{
           return producerProxyFactory.newInstance(producer,beanFactory);
        }catch (Exception e){
            throw new BindingException("error getting producer instance, Cause :" + e,e);
        }
    }

    public <T> boolean hasMapper(Class<T> type) {
        return producerMappers.containsKey(type);
    }

    public <T> void addMapper(Class<T> type) {
        if (type.isInterface()) {
            if (hasMapper(type)) {
                throw new BindingException("Type " + type + " is already known to the ProducerRegistry.");
            }
            boolean loadCompleted = false;
            try {
                producerMappers.put(type, new ProducerProxyFactory(type));
                // It's important that the type is added before the parser is run
                // otherwise the binding may automatically be attempted by the
                // mapper parser. If the type is already known, it won't try.
                /*MapperAnnotationBuilder parser = new MapperAnnotationBuilder(config, type);
                parser.parse();*/
                loadCompleted = true;
            } finally {
                if (!loadCompleted) {
                    producerMappers.remove(type);
                }
            }
        }
    }

}
