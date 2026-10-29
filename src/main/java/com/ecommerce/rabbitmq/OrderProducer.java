package com.ecommerce.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * =========================================================================
 * LAB 3: OrderProducer with Fanout & Topic Exchange Publishing
 * =========================================================================
 *
 * AMQP 0-9-1 EXCHANGE TYPES & ROUTING MECHANICS:
 *
 * 1. Direct Exchange ('orders.direct'):
 *    - Strict equality: routingKey on Basic.Publish == bindingKey on Queue.Bind.
 *
 * 2. Fanout Exchange ('payment.fanout.ex'):
 *    - 1-to-Many Broadcast.
 *    - The broker COMPLETELY IGNORES the routing key during publication.
 *    - Dispatches a copy of the message to EVERY queue bound to it.
 *
 * 3. Topic Exchange ('logs.topic.ex'):
 *    - Pattern-matching pub/sub based on dot-delimited tokens (e.g., '<region>.<service>.<severity>').
 *    - Wildcard Rules in AMQP:
 *      * (asterisk): Matches EXACTLY ONE word.
 *        Example: '*.payment.error' matches 'eu.payment.error' and 'us.payment.error',
 *                 but NOT 'payment.error' or 'eu.west.payment.error'.
 *      # (hash): Matches ZERO OR MORE words.
 *        Example: 'eu.#' matches 'eu.order.created', 'eu.payment.error', and 'eu'.
 *    - Internal Broker Trie:
 *      RabbitMQ uses a high-performance routing trie in Erlang memory to match topic patterns
 *      in sub-microsecond time.
 */
public class OrderProducer {

    public static final String DIRECT_EXCHANGE = "orders.direct";
    public static final String FANOUT_EXCHANGE = "payment.fanout.ex";
    public static final String TOPIC_EXCHANGE = "logs.topic.ex";

    public static final String PAYMENT_QUEUE = "orders.payment";
    public static final String ORDER_ROUTING_KEY = "order.created";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.confirmSelect();

            // 1. Declare Direct Exchange & Quorum Queue for Order Processing
            channel.exchangeDeclare(DIRECT_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-queue-type", "quorum");
            channel.queueDeclare(PAYMENT_QUEUE, true, false, false, queueArgs);
            channel.queueBind(PAYMENT_QUEUE, DIRECT_EXCHANGE, ORDER_ROUTING_KEY);

            // 2. Declare Fanout Exchange (Broadcasts completed payments)
            channel.exchangeDeclare(FANOUT_EXCHANGE, BuiltinExchangeType.FANOUT, true, false, null);

            // 3. Declare Topic Exchange (Broadcasts regional system & audit logs)
            channel.exchangeDeclare(TOPIC_EXCHANGE, BuiltinExchangeType.TOPIC, true, false, null);

            System.out.println("[Producer] Topologies declared: Direct, Fanout, and Topic Exchanges.");

            // 4. Publish 2 Orders to Direct Exchange
            System.out.println("\n--- [1] Publishing Orders to Direct Exchange ---");
            publishOrder(channel, "{\"orderId\":\"ORD-3001\",\"item\":\"Gaming Mouse\",\"amount\":79.99}");
            publishOrder(channel, "{\"orderId\":\"ORD-3002\",\"item\":\"4K Monitor\",\"amount\":349.99}");

            // 5. Publish Regional Topic Logs with different routing keys
            System.out.println("\n--- [2] Publishing Regional Logs to Topic Exchange ---");
            publishTopicLog(channel, "us.order.info", "US-East region: New order pipeline initialized");
            publishTopicLog(channel, "eu.payment.error", "EU-Central region: Payment gateway timeout detected");
            publishTopicLog(channel, "us.payment.error", "US-West region: Card processor rejected transaction");
            publishTopicLog(channel, "eu.order.warn", "EU-West region: Low inventory threshold warning");
            publishTopicLog(channel, "asia.order.info", "Asia region: Order batch dispatched");

            channel.waitForConfirmsOrDie(5000);
            System.out.println("\n[Producer] [✓] All orders and topic logs successfully published & confirmed!");

        } catch (Exception e) {
            System.err.println("[Producer] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void publishOrder(Channel channel, String payload) throws Exception {
        channel.basicPublish(
                DIRECT_EXCHANGE,
                ORDER_ROUTING_KEY,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                payload.getBytes(StandardCharsets.UTF_8)
        );
        System.out.println("  -> Dispatched Order: " + payload);
    }

    private static void publishTopicLog(Channel channel, String routingKey, String logMessage) throws Exception {
        channel.basicPublish(
                TOPIC_EXCHANGE,
                routingKey,
                null,
                logMessage.getBytes(StandardCharsets.UTF_8)
        );
        System.out.println("  -> Dispatched Topic Log [" + routingKey + "]: " + logMessage);
    }
}
