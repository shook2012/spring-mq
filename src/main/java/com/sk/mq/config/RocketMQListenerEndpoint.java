package com.sk.mq.config;


import com.sk.mq.core.RocketMQConsumer;

public interface RocketMQListenerEndpoint {

    String getId();

    String getTopic();

    void setConsumer(final RocketMQConsumer anConsumer);

    RocketMQConsumer getConsumer();

}
