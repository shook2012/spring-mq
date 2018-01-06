package com.sk.mq.message;

/**
 * Created by samfan on 2017-08-29.
 */
public class MQSendResult {

    private SendStatus sendStatus;

    private String messageId;

    public enum SendStatus {
        SEND_OK,
        SEND_FAILURE;

        private SendStatus() {
        }
    }

    public MQSendResult() {
    }
    public MQSendResult(SendStatus sendStatus, String messageId) {
        this.sendStatus = sendStatus;
        this.messageId = messageId;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}

