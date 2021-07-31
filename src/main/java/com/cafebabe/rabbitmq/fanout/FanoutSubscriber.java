package com.cafebabe.rabbitmq.fanout;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FanoutSubscriber {

    private String subscriberName;

    public FanoutSubscriber(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    private void receive() throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明一个随机的队列
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, FanoutPublisher.EXCHANGE_NAME, "");
        channel.basicConsume(queueName, false, RabbitmqUtil.getDefaultAckConsumer(channel, subscriberName));
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        FanoutSubscriber fanoutSubscriber1 = new FanoutSubscriber("订阅者1");
        FanoutSubscriber fanoutSubscriber2 = new FanoutSubscriber("订阅者2");
        fanoutSubscriber1.receive();
        fanoutSubscriber2.receive();
    }
}
