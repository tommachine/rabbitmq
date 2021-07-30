package com.cafebabe.rabbitmq.confirmpublish;

import com.cafebabe.rabbitmq.utils.RabbitmqUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

public class ConfirmPublish {
    public static final String QUEUE_NAME = "confirm_queue";

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        //单独确认花费时间为1391ms
        //confirmByOne();
        //批量确认花费时间为：90ms
        //confirmByMulti();
        //异步确认花费时间为：40ms
        confirmByAsync();
    }

    private static void confirmByAsync() throws IOException, TimeoutException, InterruptedException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.confirmSelect();
        ConcurrentSkipListMap<Long, String> messageData = new ConcurrentSkipListMap<Long, String>();
        channel.addConfirmListener(new ConfirmListener() {

            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> toRemoveData = messageData.headMap(deliveryTag);
                    toRemoveData.clear();
                } else {
                    messageData.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {

            }
        });
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String message = i + "";
            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            messageData.put(nextPublishSeqNo, message);

        }
        long end = System.currentTimeMillis();
        System.out.println("异步确认花费时间为：" + (end - begin) + "ms");
        channel.close();
        connection.close();
    }


    private static void confirmByMulti() throws IOException, TimeoutException, InterruptedException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String message = i + "";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            if (i % 99 == 0) {
                channel.waitForConfirms();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("批量确认花费时间为：" + (end - begin) + "ms");
        channel.close();
        connection.close();
    }

    public static void confirmByOne() throws IOException, TimeoutException, InterruptedException {
        Connection connection = RabbitmqUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String message = i + "";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirms();
        }
        long end = System.currentTimeMillis();
        System.out.println("单独确认花费时间为：" + (end - begin) + "ms");
        channel.close();
        connection.close();
    }
}
