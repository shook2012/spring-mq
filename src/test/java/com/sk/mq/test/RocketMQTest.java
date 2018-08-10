package com.sk.mq.test;

import com.sk.mq.bean.TestBean;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Date;

/**
 * Created by fangyt on 2018/8/9.
 */
public class RocketMQTest {

    private ClassPathXmlApplicationContext context;

    @Before
    public void init(){
        context = new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml"});
        context.start();
    }

    @Test
    public void testProducerAndConsumer(){
        TestMessager testMessager = (TestMessager)context.getBean("testMessager");
        TestBean testBean = new TestBean(1,"小五",new Date());

        testMessager.sendMessage(testBean);

        while (true){

        }

    }

}
