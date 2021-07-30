package com.cafebabe.rabbitmq.utils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitmqUtil {
    public static final ConnectionFactory connectionFactory = new ConnectionFactory();

    static {
        connectionFactory.setHost("10.128.4.36");
        connectionFactory.setUsername("rabbitmq");
        connectionFactory.setPassword("rabbitmq");
    }


    public static Connection getConnection() throws IOException, TimeoutException {
        return connectionFactory.newConnection();
    }

    public static DefaultConsumer getDefaultConsumer(Channel channel, String consumerName) {
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(consumerName + "接收到消息：" + new String(body));
            }
        };

        return consumer;
    }

    public static DefaultConsumer getDefaultAckConsumer(Channel channel, String consumerName) {
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println(consumerName + "接收到消息：" + new String(body));
                channel.basicAck(deliveryTag,false);
            }
        };
        return consumer;
    }

}
