package com.sk.mq.test;

import com.sk.mq.annotation.AsyncMQ;
import com.sk.mq.bean.ConsumerConstants;
import com.sk.mq.bean.TestBean;
import com.sk.mq.bean.TopicTagConstants;

/**
 * Created by fangyt on 2018/8/9.
 */
public interface TestMessager {

    @AsyncMQ(topic = TopicTagConstants.TOPIC_TEST,tags = TopicTagConstants.TAG_TEST_SEND_MESSAGE,consumer = ConsumerConstants.TEST_CONSUMER)
    void sendMessage(TestBean testBean);

}
