package com.sk.mq.test;

import com.sk.mq.bean.TestBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Created by fangyt on 2018/8/9.
 */
@Component
@Slf4j
public class TestMessageHandler implements TestMessager {

    @Override
    public void sendMessage(TestBean testBean) {
        log.info("testBean:{}",testBean);
    }
}
