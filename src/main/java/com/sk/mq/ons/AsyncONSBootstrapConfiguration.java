package com.sk.mq.ons;

import com.sk.mq.annotation.EnableAsyncONS;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * @see EnableAsyncONS
 */
@Configuration
public class AsyncONSBootstrapConfiguration {

	public final static String ONS_TOPIC_PREFIX_KEY = "${ons.mq.topicPrefix}";

	public static final String ROCKETMQ_ANNOTATION_PROCESSOR_BEAN_NAME =
			"org.springframework.rocketmq.config.internalAsncMQAnnotationProcessor";

	@Bean(name = ROCKETMQ_ANNOTATION_PROCESSOR_BEAN_NAME)
    @Role(BeanDefinition.ROLE_SUPPORT)
	public AsyncONSAnnotationBeanPostProcessor rocketMQListenerAnnotationProcessor() {
		return new AsyncONSAnnotationBeanPostProcessor();
	}

}
