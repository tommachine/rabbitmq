package com.cafebabe.rabbitmq.delayedmessage;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @author cafebabe
 */
public class DelayedMessagePublisher {
    public static final String DELAYED_EXCHANGE_NAME = "delayed_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        Map<String,Object> arguments = new HashMap<>(16);
        arguments.put("x-delayed-type", "direct");
        channel.exchangeDeclare(DELAYED_EXCHANGE_NAME,"x-delayed-message",true,false,arguments);
        Map<String, Object> headers = new HashMap<>();

        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入延时消费时间：");
        while (scanner.hasNext()){
            int next = scanner.nextInt();
            headers.put("x-delay", 1000 * next);
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder().headers(headers);
            channel.basicPublish(DELAYED_EXCHANGE_NAME, "delay",builder.build(),("延时时间为"+next+"秒的消息").getBytes(StandardCharsets.UTF_8) );
            System.out.println("发送延时消息生成");
            System.out.println("请输入延时消费时间：");
        }
    }
}
