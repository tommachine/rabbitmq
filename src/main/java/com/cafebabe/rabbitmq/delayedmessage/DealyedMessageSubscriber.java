package com.cafebabe.rabbitmq.delayedmessage;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DealyedMessageSubscriber {

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, DelayedMessagePublisher.DELAYED_EXCHANGE_NAME, "delay");
        channel.basicConsume(queue, true, RabbitmqUtil.getDefaultConsumer(channel, "延时消息消费者"));
    }
}
