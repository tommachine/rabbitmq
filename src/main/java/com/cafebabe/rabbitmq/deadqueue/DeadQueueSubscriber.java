package com.cafebabe.rabbitmq.deadqueue;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class DeadQueueSubscriber {
    public static final String NORMAL_QUEUE = "normal_queue";
    public static final String DEAD_QUEUE = "dead_queue";
    public static final String NORMAL_ROUTING_KEY = "normal";

    public DeadQueueSubscriber(String normalSubscriberName) {
        this.normalSubscriberName = normalSubscriberName;
    }

    private String normalSubscriberName;

    private void consumeNormalQueue(Channel channel) throws IOException, TimeoutException {
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(normalSubscriberName + "接收到消息：" + new String(body));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(NORMAL_QUEUE, false, consumer);
    }

    private Channel initQueue() throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明一个最大容量为6的正常队列，绑定到正常交换机上面
        Map<String, Object> arguments = new HashMap<>(16);
        arguments.put("x-dead-letter-exchange", DeadQueuePublisher.DEAD_EXCHANGE_NAME);
        arguments.put("x-dead-letter-routing-key", DeadQueuePublisher.NORMAL_TO_DEAD_ROUTING_KEY);
        arguments.put("x-max-length", 6);
        channel.queueDeclare(NORMAL_QUEUE, true, false, false, arguments);
        channel.queueBind(NORMAL_QUEUE, DeadQueuePublisher.NORMAL_EXCHANGE_NAME, NORMAL_ROUTING_KEY);

        //声明一个死信队列,和死信交换机绑定
        channel.queueDeclare(DEAD_QUEUE, true, false, false, null);
        channel.queueBind(DEAD_QUEUE, DeadQueuePublisher.DEAD_EXCHANGE_NAME, DeadQueuePublisher.NORMAL_TO_DEAD_ROUTING_KEY);

        return channel;
    }

    private void consumeDeadQueue(Channel channel) throws IOException, TimeoutException {
        channel.basicConsume(DEAD_QUEUE, true, RabbitmqUtil.getDefaultConsumer(channel, "死信" + normalSubscriberName));
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        DeadQueueSubscriber deadQueueSubscriber = new DeadQueueSubscriber("队列测试");
        Channel channel = deadQueueSubscriber.initQueue();
        //测试ttl超时时可以注释下面方法
        deadQueueSubscriber.consumeNormalQueue(channel);
        deadQueueSubscriber.consumeDeadQueue(channel);
    }
}
