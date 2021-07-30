package com.cafebabe.rabbitmq.workqueue;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author akun
 */
public class ReceiveQueue {

    private String receiveQueueName;
    
    public ReceiveQueue(String receiveQueueName) {
        this.receiveQueueName = receiveQueueName;
    }

    public void run() throws IOException, TimeoutException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        channel.basicConsume(Worker.QUEUE_NAME, false, RabbitmqUtil.getDefaultAckConsumer(channel, receiveQueueName));
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        ReceiveQueue receiveQueue1 = new ReceiveQueue("工作队列1");
        ReceiveQueue receiveQueue2 = new ReceiveQueue("工作队列2");
        receiveQueue1.run();
        receiveQueue2.run();
    }
}
