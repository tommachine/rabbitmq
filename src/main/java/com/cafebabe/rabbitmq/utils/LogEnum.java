package com.cafebabe.rabbitmq.utils;

public enum LogEnum {

    ERROR("error"), INFO("info"), WARNING("warning");

    private String value;

    LogEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
