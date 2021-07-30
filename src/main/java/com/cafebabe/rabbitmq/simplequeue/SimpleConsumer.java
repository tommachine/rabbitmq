package com.cafebabe.rabbitmq.simplequeue;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SimpleConsumer {
    public static final String SIMPLE_CONSUMER_NAME = "simple_consumer";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.basicConsume(SimpleProducer.QUEUE_NAME,true,RabbitmqUtil.getDefaultConsumer(channel,SIMPLE_CONSUMER_NAME));
    }
}
