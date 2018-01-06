package com.sk.mq.ons;

import com.sk.mq.core.ONSConsumer;
import com.sk.mq.core.ONSMessageListener;
import com.sk.mq.annotation.AsyncMQ;
import com.sk.mq.support.MethodIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.access.BootstrapException;
import org.springframework.beans.factory.config.*;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.sk.mq.ons.AsyncONSBootstrapConfiguration.ONS_TOPIC_PREFIX_KEY;


/**
 * @author samfan
 */
public class AsyncONSAnnotationBeanPostProcessor
        implements BeanPostProcessor,Ordered, BeanFactoryAware {

    private final Set<Class<?>> nonAnnotatedClasses = Collections
            .newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>(64));

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private BeanFactory beanFactory;

    private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

    private BeanExpressionContext expressionContext;

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    /**
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            ConfigurableListableBeanFactory factory = ((ConfigurableListableBeanFactory) beanFactory);
            this.resolver = factory.getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            final Map<Method, Set<AsyncMQ>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    new MethodIntrospector.MetadataLookup<Set<AsyncMQ>>() {
                        @Override
                        public Set<AsyncMQ> inspect(Method method) {
                            Set<AsyncMQ> listenerMethods = findListenerAnnotations(method);
                            return (!listenerMethods.isEmpty() ? listenerMethods : null);
                        }
                    });

            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
            } else {
                // Non-empty set of methods
                for (Map.Entry<Method, Set<AsyncMQ>> entry : annotatedMethods.entrySet()) {
                    Method method = entry.getKey();
                    for (AsyncMQ listener : entry.getValue()) {
                        try {
                            //接口产生的producer代理类不能订阅
                            if(!(bean instanceof Proxy)){
                                processRocketMQListener(listener, method, bean, beanName);
                            }else{
                                logger.warn("bean:{} is an interface proxy",bean.getClass().getName());
                            }
                        } catch (Exception e) {
                            throw new BootstrapException("process @AsyncMQ error.", e);
                        }
                    }
                }
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug(annotatedMethods.size() + " @AsyncMQ methods processed on bean '"
                            + beanName + "': " + annotatedMethods);
                }
            }
        }
        return bean;
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<AsyncMQ> findListenerAnnotations(Method method) {
        Set<AsyncMQ> listeners = new HashSet<AsyncMQ>();
        AsyncMQ ann = AnnotationUtils.findAnnotation(method, AsyncMQ.class);
        if (ann != null) {
            listeners.add(ann);
        }
        return listeners;
    }

    private Set<AsyncMQ> findAsncMQAnnotations(Method method) {
            Set<AsyncMQ> annotationMethods = new HashSet<AsyncMQ>();
            AsyncMQ ann = AnnotationUtils.findAnnotation(method, AsyncMQ.class);
            if (ann != null) {
                annotationMethods.add(ann);
            }
            return annotationMethods;
    }

    protected void processRocketMQListener(AsyncMQ mqAnnotation, Method method, Object bean,
                                           String beanName) throws Exception {
        Method methodToUse = checkProxy(method, bean);
        processListener(methodToUse, mqAnnotation, bean);
    }

    private Method checkProxy(Method targetMethod, Object targetBean) {
        Method method = targetMethod;
        if (AopUtils.isJdkDynamicProxy(targetBean)) {
            try {
                // Found a @AsyncMQ method on the target class for this
                // JDK proxy ->
                // is it also present on the proxy itself?
                method = targetBean.getClass().getMethod(method.getName(), method.getParameterTypes());
                Class<?>[] proxiedInterfaces = ((Advised) targetBean).getProxiedInterfaces();
                for (Class<?> iface : proxiedInterfaces) {
                    try {
                        method = iface.getMethod(method.getName(), method.getParameterTypes());
                        break;
                    } catch (NoSuchMethodException noMethod) {
                    }
                }
            } catch (SecurityException ex) {
                ReflectionUtils.handleReflectionException(ex);
            } catch (NoSuchMethodException ex) {
                throw new IllegalStateException(String.format(
                        "@AsyncMQ method '%s' found on anBean target class '%s', "
                                + "but not found in any interface(s) for anBean JDK proxy. Either "
                                + "pull the method up to an interface or switch to subclass (CGLIB) "
                                + "proxies by setting proxy-target-class/proxyTargetClass " + "attribute to 'true'",
                        method.getName(), method.getDeclaringClass().getSimpleName()), ex);
            }
        }
        return method;
    }

    protected void processListener(Method method, AsyncMQ mqAnnotation,
            Object bean) throws Exception {
        ONSMessageListener messageListener = new ONSMessageListener(bean,method,resolveTopic(mqAnnotation.topic()),resolveExpression(mqAnnotation.tags()));
        ONSConsumer consumer = resolveConsumer(mqAnnotation);
        consumer.addSubscriber(messageListener);
    }

    private ONSConsumer resolveConsumer(AsyncMQ mqAnnotation) {
        String consumer = mqAnnotation.consumer();
        if (!StringUtils.isEmpty(consumer)) {
            return (ONSConsumer) this.beanFactory.getBean(consumer);
        }
        return null;
    }

    private String resolveTopic(String  topicExpr) {
        String topicPrefix = resolveExpression(ONS_TOPIC_PREFIX_KEY);
        if(topicPrefix.equals(ONS_TOPIC_PREFIX_KEY)){
            topicPrefix = "";
        }
        return topicPrefix + resolveExpression(topicExpr);
    }

    /**
     * Resolve the specified anValue if possible.
     *
     * @param value
     *            the anValue to resolve
     * @return the resolved anValue
     * @see ConfigurableBeanFactory#resolveEmbeddedValue
     */
    private String resolveExpression(String value) {
        if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
            return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
        }
        return value;
    }

}
