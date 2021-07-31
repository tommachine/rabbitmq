package com.cafebabe.rabbitmq.direct;

import com.cafebabe.rabbitmq.utils.LogEnum;
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
 * @author Administrator
 */
public class DirectPublisher {

    public static final String EXCHANGE_NAME = "log_direct";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT.getType());
        Scanner scanner = new Scanner(System.in);
        print("请输入routingkey：");
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            String message = null;
            String routingKey = null;
            switch (next) {
                case "info":
                    routingKey = LogEnum.INFO.getValue();
                    print("请输入消息体：");
                    message = scanner.nextLine();
                    break;
                case "warning":
                    routingKey = LogEnum.WARNING.getValue();
                    print("请输入消息体：");
                    message = scanner.nextLine();
                    break;
                case "error":
                    routingKey = LogEnum.ERROR.getValue();
                    print("请输入消息体：");
                    message = scanner.nextLine();
                    break;
                default:
                    System.out.println("routingkey仅支持info,warning,error");
                    print("请输入routingkey：");
                    break;
            }
            if (null == routingKey) {
                continue;
            }
            System.out.println("投递消息的路由key：" + routingKey);
            System.out.println("投递消息的消息体：" + message);
            channel.basicPublish(EXCHANGE_NAME, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            print("请输入routingkey：");
        }

    }

    public static void print(String args) {
        System.out.println(args);
    }
}
