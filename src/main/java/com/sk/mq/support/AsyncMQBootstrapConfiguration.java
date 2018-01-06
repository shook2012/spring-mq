package com.sk.mq.support;

import com.sk.mq.annotation.EnableAsyncMQ;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * @see EnableAsyncMQ
 */
@Configuration
public class AsyncMQBootstrapConfiguration {

	public final static String ROCKETMQ_TOPIC_PREFIX_KEY = "${rocket.mq.topicPrefix}";

	public static final String ROCKETMQ_ANNOTATION_PROCESSOR_BEAN_NAME =
			"org.springframework.rocketmq.config.internalAsncMQAnnotationProcessor";

	@Bean(name = ROCKETMQ_ANNOTATION_PROCESSOR_BEAN_NAME)
    @Role(BeanDefinition.ROLE_SUPPORT)
	public AsyncMQAnnotationBeanPostProcessor rocketMQListenerAnnotationProcessor() {
		return new AsyncMQAnnotationBeanPostProcessor();
	}

}
