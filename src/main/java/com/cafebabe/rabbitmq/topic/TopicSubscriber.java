package com.cafebabe.rabbitmq.topic;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TopicSubscriber {
    private String subscriberName;
    private String routingKey;

    public TopicSubscriber(String subscriberName, String routingKey) {
        this.subscriberName = subscriberName;
        this.routingKey = routingKey;
    }

    /**
     * run topic mode subscriber
     *
     * @throws IOException
     * @throws TimeoutException
     */
    public void run() throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();

        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, TopicPublisher.TOPIC_QUEUE_NAME, routingKey);
        channel.basicConsume(queue, true, RabbitmqUtil.getDefaultConsumer(channel, subscriberName));
    }

    /**
     * construct different routingKey subscriber
     *
     * @param args
     * @throws IOException
     * @throws TimeoutException
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        TopicSubscriber subscriber1 = new TopicSubscriber("主题消费者1", "topic.log.#");
        TopicSubscriber subscriber2 = new TopicSubscriber("主题消费者2", "topic.log.*");
        TopicSubscriber subscriber3 = new TopicSubscriber("主题消费者3", "topic.log.info");
        subscriber1.run();
        subscriber2.run();
        subscriber3.run();
    }
}
