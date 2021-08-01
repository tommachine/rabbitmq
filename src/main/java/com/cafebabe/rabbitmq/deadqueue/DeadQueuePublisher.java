package com.cafebabe.rabbitmq.deadqueue;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @author cafebabe
 */
public class DeadQueuePublisher {

    public static final String NORMAL_EXCHANGE_NAME = "normal_exchange";

    public static final String DEAD_EXCHANGE_NAME = "dead_exchange";

    public static final String NORMAL_TO_DEAD_ROUTING_KEY = "normal_to_dead";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        Map<String, Object> argument = new HashMap<>(16);
        channel.exchangeDeclare(NORMAL_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, argument);

        channel.exchangeDeclare(DEAD_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null);

        System.out.println("请输入死信队列类型：");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            switch (next) {
                case "ttl":
                    publishTtlMsg(channel);
                    System.out.println("请输入死信队列类型：");
                    break;
                case "max-priority":
                    publishMaxPriorityMsg(channel);
                    System.out.println("请输入死信队列类型：");
                    break;
                case "reject":
                    publishRejectMsg(channel);
                    System.out.println("请输入死信队列类型：");
                    break;
                default:
                    System.out.println("仅支持ttl,max-priority,reject");
                    System.out.println("请输入死信队列类型：");
                    break;
            }
        }
    }

    private static void publishTtlMsg(Channel channel) throws IOException {
        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder().expiration("5000").build();
            channel.basicPublish(NORMAL_EXCHANGE_NAME, "normal", props, ("TTL为5s的消息" + i).getBytes(StandardCharsets.UTF_8));
        }

    }

    private static void publishMaxPriorityMsg(Channel channel) throws IOException {
        //正常队列设置max-priority = 6
        for (int i = 0; i < 10; i++) {
            channel.basicPublish(NORMAL_EXCHANGE_NAME, "normal", null, ("max-priority为6的消息" + i).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void publishRejectMsg(Channel channel) throws IOException {
        for (int i = 0; i < 10; i++) {
            channel.basicPublish(NORMAL_EXCHANGE_NAME, "normal", null, ("需要消费端reject的消息" + i).getBytes(StandardCharsets.UTF_8));
        }
    }


}
