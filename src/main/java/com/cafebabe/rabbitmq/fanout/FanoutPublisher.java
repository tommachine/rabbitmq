package com.cafebabe.rabbitmq.fanout;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class FanoutPublisher {

    public static final String EXCHANGE_NAME = "log_fanout";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT.getType());
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            channel.basicPublish(EXCHANGE_NAME, "", MessageProperties.PERSISTENT_TEXT_PLAIN, next.getBytes(StandardCharsets.UTF_8));
        }

    }
}
