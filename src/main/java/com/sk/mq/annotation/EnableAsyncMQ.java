package com.sk.mq.annotation;

import com.sk.mq.support.AsyncMQBootstrapConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 使用@Configuration 来启用RocketMQ
 * 通过 AsyncMQBootstrapConfiguration 加载 AsyncMQAnnotationBeanPostProcessor
 *
 * @author samfan
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(AsyncMQBootstrapConfiguration.class)
public @interface EnableAsyncMQ {
}