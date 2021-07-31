package com.cafebabe.rabbitmq.direct;

import com.cafebabe.rabbitmq.utils.LogEnum;
import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Administrator
 */
public class DirectSubscriber {

    private String subscriberName;

    private String routingKey;

    public DirectSubscriber(String subscriberName, String routingKey) {
        this.subscriberName = subscriberName;
        this.routingKey = routingKey;
    }

    public DirectSubscriber(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    private void receive() throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明一个随机的队列
        String queueName = channel.queueDeclare().getQueue();
        String[] split = routingKey.split(",");
        for (String s : split) {
            channel.queueBind(queueName, DirectPublisher.EXCHANGE_NAME, s);
        }
        channel.basicConsume(queueName, false, RabbitmqUtil.getDefaultAckConsumer(channel, subscriberName));
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        DirectSubscriber fanoutSubscriber1 = new DirectSubscriber("订阅者1", LogEnum.INFO.getValue());
        DirectSubscriber fanoutSubscriber2 = new DirectSubscriber("订阅者2","warning,error");
        fanoutSubscriber1.receive();
        fanoutSubscriber2.receive();
    }
}
