package com.sk.mq.support;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.StringUtils;

/**
 * Created by samfan on 2017-04-22.
 */
public class MQInterfaceScannerConfigurer implements BeanFactoryPostProcessor,ApplicationContextAware {

    private String basePackage;

    private String producerBeanName;

    private ApplicationContext applicationContext;

    public void setBasePackage(String basePackage) {
        this.basePackage = basePackage;
    }

    public void setProducerBeanName(String producerBeanName) {
        this.producerBeanName = producerBeanName;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        RocketMQInterfaceScanner interfaceScanner = new RocketMQInterfaceScanner((BeanDefinitionRegistry)beanFactory);
        interfaceScanner.setResourceLoader(applicationContext);
        interfaceScanner.setProducerBeanName(producerBeanName);
        interfaceScanner.scan(StringUtils.tokenizeToStringArray(this.basePackage, ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS));
    }
}
