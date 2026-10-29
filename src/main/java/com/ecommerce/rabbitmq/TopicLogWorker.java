package com.ecommerce.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/**
 * =========================================================================
 * LAB 3: TopicLogWorker (Topic Wildcard Routing with * and #)
 * =========================================================================
 *
 * AMQP MECHANICS: TOPIC EXCHANGE PATTERN MATCHING
 *
 * 1. Routing Key Structure:
 *    - Dot-delimited words up to 255 bytes (e.g. '<region>.<service>.<severity>')
 *
 * 2. Wildcard Matching:
 *    - '*' (asterisk): Substitutes for EXACTLY ONE word.
 *    - '#' (hash): Substitutes for ZERO OR MORE words.
 *
 * 3. Dynamic Temporary Queues:
 *    - channel.queueDeclare().getQueue() declares a temporary, server-named,
 *      exclusive, auto-delete queue. When this worker process exits, the queue
 *      is automatically cleaned up by RabbitMQ!
 */
public class TopicLogWorker {

    public static final String TOPIC_EXCHANGE = "logs.topic.ex";

    public static void main(String[] args) throws Exception {
        // Accepts pattern from command line, defaults to '#' (all messages)
        String bindingPattern = (args.length > 0) ? args[0] : "#";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1. Declare Topic Exchange
        channel.exchangeDeclare(TOPIC_EXCHANGE, BuiltinExchangeType.TOPIC, true, false, null);

        // 2. Declare temporary server-named exclusive queue
        String tempQueue = channel.queueDeclare().getQueue();

        // 3. Bind the queue to Topic Exchange with the requested wildcard pattern
        channel.queueBind(tempQueue, TOPIC_EXCHANGE, bindingPattern);

        System.out.println(" [*] TopicLogWorker active!");
        System.out.println(" [*] Bound to Exchange: '" + TOPIC_EXCHANGE + "'");
        System.out.println(" [*] Subscribed to Pattern: [" + bindingPattern + "]");
        System.out.println(" [*] Waiting for matching logs. Press CTRL+C to exit.\n");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String routingKey = delivery.getEnvelope().getRoutingKey();
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(">>> [MATCH: " + routingKey + "] -> " + message);
        };

        channel.basicConsume(tempQueue, true, deliverCallback, consumerTag -> {});

        Thread.currentThread().join();
    }
}
