package com.sk.mq.core;


import com.sk.mq.message.MQSendResult;
import com.sk.mq.message.MessageBean;

/**
 * Created by samfan on 2017-08-29.
 */
public interface MQProducer {

    /**
     *
     * @param messageBean
     * @return
     * @throws Exception
     */
    MQSendResult send(MessageBean messageBean) throws Exception;

    /**
     *
     * @param messageBean
     * @throws Exception
     */
    void sendAsync(MessageBean messageBean) throws Exception;

    /**
     *
     * @param messageBean
     * @param shardingKey
     * @return
     * @throws Exception
     */
    MQSendResult sendOrderMessage(MessageBean messageBean, Object shardingKey) throws Exception;
}
