package com.sk.mq.exceptions;

/**
 * Created by samfan on 2017-04-24.
 * 消费时出现异常可上抛出此异常，令消息consume later
 * 具体的重试次数需要自己控制
 */
public class ConsumeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConsumeException() {
        super();
    }

    public ConsumeException(String message) {
        super(message);
    }

    public ConsumeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumeException(Throwable cause) {
        super(cause);
    }
}
