package com.sk.mq.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by samfan on 2017-08-11.
 * Log4j ThreadContext
 * 适配log4j1.x 及 2.x
 */
public class LogThreadContext {

    private static final Logger logger = LoggerFactory.getLogger(LogThreadContext.class);

    private static Class<?> threadContext = null;

    private static String DEFAULT_NDC = "";

    static {
        try {
            threadContext = Class.forName("org.apache.logging.log4j.ThreadContext");
        } catch (ClassNotFoundException e) {
            //logger
            logger.warn("当前环境找不到log4j2依赖.");
            try {
                threadContext = Class.forName("org.apache.log4j.NDC");
            } catch (ClassNotFoundException e1) {
                logger.error("当前环境找不到log4j1.x依赖，无法使用NDC。");
                //error
            }
        }
    }

    /**
     *
     * @return
     */
    public static String pop()
    {
        return invoke("pop");
    }

    /**
     *
     * @return
     */
    public static String peek()
    {
        return invoke("peek");
    }

    /**
     *
     * @param message
     */
    public static void push(String message)
    {
        if(threadContext == null){
            return;
        }
        try {
            threadContext.getMethod("push",String.class).invoke(null,message);
        } catch (NoSuchMethodException e) {
            logger.error("没找到相关方法",e);
        } catch (InvocationTargetException e) {
            logger.error("InvocationTargetException:",e);
        } catch (IllegalAccessException e) {
            logger.error("IllegalAccessException:",e);
        }
    }

    /**
     *
     * @param methodName
     * @return
     */
    private static String invoke(String methodName) {
        if(threadContext == null){
            return DEFAULT_NDC;
        }
        String ndc = DEFAULT_NDC;
        try {
            ndc = (String) threadContext.getMethod(methodName).invoke(null);
        } catch (NoSuchMethodException e) {
            logger.error("没找到相关方法",e);
        } catch (InvocationTargetException e) {
            logger.error("InvocationTargetException:",e);
        } catch (IllegalAccessException e) {
            logger.error("IllegalAccessException:",e);
        }
        return ndc;
    }


}
