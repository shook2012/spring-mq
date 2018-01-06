package com.sk.mq.exceptions;

/**
 * Created by samfan on 2017-04-24.
 */
public class BindingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BindingException() {
        super();
    }

    public BindingException(String message) {
        super(message);
    }

    public BindingException(String message, Throwable cause) {
        super(message, cause);
    }

    public BindingException(Throwable cause) {
        super(cause);
    }
}
