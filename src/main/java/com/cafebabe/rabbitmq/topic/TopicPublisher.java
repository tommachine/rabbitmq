package com.cafebabe.rabbitmq.topic;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @author cafebabe
 */
public class TopicPublisher {

    public static final String TOPIC_QUEUE_NAME = "log.topic";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(TOPIC_QUEUE_NAME, BuiltinExchangeType.TOPIC);

        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入routingKey:");
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            if (null == next || "".equals(next)) {
                System.out.println("请输入routingKey:");
                continue;
            }
            System.out.println("请输入需要发送的信息:");
            String message = scanner.nextLine();
            channel.basicPublish(TOPIC_QUEUE_NAME, next, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("消息发送成功!");
            System.out.println("请输入routingKey:");
        }

    }
}
