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
 * LAB 2: OrderProducer with Quorum Queues & Publisher Confirms
 * =========================================================================
 *
 * AMQP 0-9-1 MECHANICS & DISTRIBUTED SYSTEMS INTERNALS:
 *
 * 1. The Immutability Rule for Queues:
 *    - In AMQP, queue declarations are idempotent IF AND ONLY IF all arguments match.
 *    - If an existing queue was declared as "classic" (Lab 1) and we now try to declare it
 *      with 'x-queue-type: quorum', RabbitMQ raises a channel exception:
 *      406 PRECONDITION_FAILED - inequivalent arg 'x-queue-type'.
 *    - Therefore, when upgrading queue types, the old queue must be deleted first
 *      via channel.queueDelete().
 *
 * 2. Modern Quorum Queues (x-queue-type = "quorum"):
 *    - Replaces deprecated Classic Mirrored Queues (HA).
 *    - Based on the Raft Consensus Algorithm.
 *    - Messages are replicated across a quorum (majority) of Erlang nodes before being
 *      considered committed.
 *    - Strictly FIFO, high data safety, resilient to network splits.
 *
 * 3. Publisher Confirms (channel.confirmSelect()):
 *    - Standard basicPublish is "fire-and-forget" over the TCP socket. The producer has
 *      zero guarantee the broker received, routed, or stored the message.
 *    - When confirmSelect() is called, the client issues a METHOD Frame: Confirm.Select.
 *    - The broker switches this channel to "confirm mode" and replies with Confirm.Select-Ok.
 *    - For every basicPublish, the broker assigns an internal sequence number (1, 2, 3...).
 *    - Once the message is written to the Raft log on disk, the broker sends back an
 *      asynchronous METHOD Frame: Basic.Ack(deliveryTag=sequenceNumber).
 *    - channel.waitForConfirmsOrDie(timeout) blocks until all published messages in the batch
 *      have received their Basic.Ack frame from the broker.
 *
 * 4. Message Durability (MessageProperties.PERSISTENT_TEXT_PLAIN):
 *    - Sets 'deliveryMode = 2' inside the AMQP Content Header frame.
 *    - Tells the broker's storage engine to write the payload to disk/Raft log rather than
 *      relying purely on RAM memory buffers.
 */
public class OrderProducer {

    public static final String EXCHANGE_NAME = "orders.direct";
    public static final String QUEUE_NAME = "orders.payment";
    public static final String ROUTING_KEY = "order.created";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            System.out.println("[Producer] Connected. Channel ID: " + channel.getChannelNumber());

            // 1. Enable Publisher Confirms on this channel
            channel.confirmSelect();
            System.out.println("[Producer] [✓] Publisher Confirms enabled (Confirm.Select wire frame sent)");

            // 2. Declare Durable Direct Exchange
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null);

            // 3. Declare Modern Quorum Queue
            // Note: If orders.payment exists from Lab 1 as a classic queue, delete it first
            // to avoid 406 PRECONDITION_FAILED when changing queue engine type to Quorum.
            try {
                channel.queueDelete(QUEUE_NAME);
                System.out.println("[Producer] Deleted legacy queue to upgrade to Quorum type.");
            } catch (Exception ignored) {
                // Queue didn't exist, proceed
            }

            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-queue-type", "quorum"); // Declares a Raft-replicated Quorum Queue

            channel.queueDeclare(QUEUE_NAME, true, false, false, queueArgs);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            System.out.println("[Producer] [✓] Declared Quorum Queue: '" + QUEUE_NAME + "' (type: quorum, durable: true)");

            // 4. Publish a batch of 6 orders to test Competing Consumers & Load Balancing
            int totalOrders = 6;
            System.out.println("\n[Producer] Publishing batch of " + totalOrders + " persistent orders...");

            for (int i = 1; i <= totalOrders; i++) {
                String orderPayload = String.format("{\"orderId\":\"ORD-%04d\",\"item\":\"Item-%d\",\"amount\":%d.00}",
                        1000 + i, i, 50 * i);

                /*
                 * MessageProperties.PERSISTENT_TEXT_PLAIN:
                 * Encodes deliveryMode = 2 in the AMQP Content Header frame.
                 * Tells the broker: "Fsync this to disk before acking."
                 */
                channel.basicPublish(
                        EXCHANGE_NAME,
                        ROUTING_KEY,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        orderPayload.getBytes(StandardCharsets.UTF_8)
                );

                System.out.println("  -> Dispatched: " + orderPayload);
            }

            // 5. Block and wait for Broker Acks for all 6 messages
            System.out.println("\n[Producer] Waiting for Broker Publisher Confirms (Basic.Ack)...");
            channel.waitForConfirmsOrDie(5000);
            System.out.println("[Producer] [✓✓✓] All 6 orders safely committed to disk by RabbitMQ!");

        } catch (Exception e) {
            System.err.println("[Producer] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
