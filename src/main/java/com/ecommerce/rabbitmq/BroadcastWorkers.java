package com.ecommerce.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

/**
 * =========================================================================
 * LAB 3: BroadcastWorkers (Email & Analytics Services bound to Fanout)
 * =========================================================================
 *
 * AMQP MECHANICS: FANOUT 1-TO-MANY SUBSCRIPTION
 *
 * 1. Queue Bindings:
 *    - Queue 'email.queue' is bound to exchange 'payment.fanout.ex'.
 *    - Queue 'analytics.queue' is bound to exchange 'payment.fanout.ex'.
 *    - Both binding keys are set to "" (ignored by fanout).
 *
 * 2. Simultaneous Independent Consumption:
 *    - When 1 message is published to 'payment.fanout.ex', RabbitMQ copies a pointer
 *      to the payload into BOTH queues.
 *    - The Email service and Analytics service each have their own independent consumers,
 *      their own deliveryTags, and their own unacknowledged state tracking.
 *    - If Email service crashes or slows down, Analytics is 100% unaffected!
 */
public class BroadcastWorkers {

    public static final String FANOUT_EXCHANGE = "payment.fanout.ex";
    public static final String EMAIL_QUEUE = "email.queue";
    public static final String ANALYTICS_QUEUE = "analytics.queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1. Declare Fanout Exchange
        channel.exchangeDeclare(FANOUT_EXCHANGE, BuiltinExchangeType.FANOUT, true, false, null);

        // 2. Declare and Bind Email Queue
        channel.queueDeclare(EMAIL_QUEUE, true, false, false, null);
        channel.queueBind(EMAIL_QUEUE, FANOUT_EXCHANGE, "");

        // 3. Declare and Bind Analytics Queue
        channel.queueDeclare(ANALYTICS_QUEUE, true, false, false, null);
        channel.queueBind(ANALYTICS_QUEUE, FANOUT_EXCHANGE, "");

        System.out.println(" [*] BroadcastWorkers started. Listening to Fanout queues:");
        System.out.println("     -> '" + EMAIL_QUEUE + "'");
        System.out.println("     -> '" + ANALYTICS_QUEUE + "'\n");

        // Consumer 1: Email Notification Service
        channel.basicConsume(EMAIL_QUEUE, false, (consumerTag, delivery) -> {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            long tag = delivery.getEnvelope().getDeliveryTag();

            System.out.println("[Email Service] Sending receipt email to customer for: " + payload);
            channel.basicAck(tag, false);
        }, tag -> {});

        // Consumer 2: Real-time Analytics & Revenue Service
        channel.basicConsume(ANALYTICS_QUEUE, false, (consumerTag, delivery) -> {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            long tag = delivery.getEnvelope().getDeliveryTag();

            System.out.println("[Analytics Service] Incrementing revenue dashboard for: " + payload);
            channel.basicAck(tag, false);
        }, tag -> {});

        Thread.currentThread().join();
    }
}
