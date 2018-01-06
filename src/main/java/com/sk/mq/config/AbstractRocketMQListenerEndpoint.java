package com.sk.mq.config;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 */
public abstract class AbstractRocketMQListenerEndpoint
		implements RocketMQListenerEndpoint, BeanFactoryAware, InitializingBean {

	private String id;

	private String topic;

	private String tags;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	@Override
	public void setBeanFactory(BeanFactory anBeanFactory) throws BeansException {
		this.beanFactory = anBeanFactory;
		if (anBeanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) anBeanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) anBeanFactory, null);
		}
	}

	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	protected BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	protected BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

	public void setId(String anId) {
		this.id = anId;
	}

	@Override
	public String getId() {
		return this.id;
	}

	public void setTopic(final String anTopic) {
		Assert.notNull(anTopic, "'Topic' must not be null");
		this.topic = anTopic;
	}

	@Override
	public String getTopic() {
		return this.topic;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	@Override
	public void afterPropertiesSet() {
		boolean topicEmpty = StringUtils.isEmpty(this.topic);

		if (topicEmpty) {
			throw new IllegalStateException("Topics must be provided but not for " + this);
		}

	}

	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append("[").append(this.id).
				append("] topic=").append(this.topic);
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
