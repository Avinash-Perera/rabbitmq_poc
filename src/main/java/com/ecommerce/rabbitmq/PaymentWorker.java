package com.ecommerce.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * =========================================================================
 * LAB 2: PaymentWorker (Competing Consumers & Fair Dispatch with basicQos)
 * =========================================================================
 *
 * AMQP 0-9-1 MECHANICS: BLIND ROUND-ROBIN VS. FAIR DISPATCH (basicQos)
 *
 * 1. The Blind Round-Robin Problem (basicQos disabled / default prefetch = 0):
 *    - In AMQP 0-9-1, RabbitMQ defaults to prefetch = 0 (unlimited).
 *    - When 6 messages hit the queue, the broker immediately dispatches them in a
 *      naive round-robin order across all active consumer TCP connections:
 *      Message 1 -> Worker A
 *      Message 2 -> Worker B
 *      Message 3 -> Worker A
 *      Message 4 -> Worker B
 *      Message 5 -> Worker A
 *      Message 6 -> Worker B
 *    - Catastrophe: If Worker A is slow (taking 3s per job) and Worker B is fast
 *      (taking 0.5s per job), Worker B finishes its 3 jobs in 1.5s and sits IDLE,
 *      while Worker A is bogged down for 9s with 3 heavy jobs queued in its TCP buffer!
 *
 * 2. The Solution: Fair Dispatch (channel.basicQos(1)):
 *    - Client sends METHOD Frame: Basic.Qos(prefetch_count=1, global=false).
 *    - The broker's Erlang queue process updates its dispatch rule for this consumer channel:
 *      "Never send more than 1 unacknowledged message to this channel at any time."
 *    - The broker delivers 1 message. It WAITS for the client to return Basic.Ack before
 *      transmitting the next message.
 *    - Result: Fast workers naturally process more jobs ("work-stealing" pattern),
 *      and slow workers are never overloaded.
 *
 * 3. Quorum Queue Topology:
 *    - Worker declares the queue with 'x-queue-type: quorum' to match the Producer.
 */
public class PaymentWorker {

    public static final String QUEUE_NAME = "orders.payment";

    public static void main(String[] args) throws Exception {
        // Parse CLI arguments: [workerName] [delayMs] [enableQos]
        String workerName = (args.length > 0) ? args[0] : "Worker-Default";
        long processingDelayMs = (args.length > 1) ? Long.parseLong(args[1]) : 3000;
        boolean enableQos = (args.length > 2) ? Boolean.parseBoolean(args[2]) : false;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declare Quorum Queue with identical arguments as Producer
        Map<String, Object> queueArgs = new HashMap<>();
        queueArgs.put("x-queue-type", "quorum");
        channel.queueDeclare(QUEUE_NAME, true, false, false, queueArgs);

        /*
         * =====================================================================
         * FAIR DISPATCH (basicQos):
         * =====================================================================
         */
        if (enableQos) {
            /*
             * basicQos(prefetchCount, global)
             * - prefetchCount = 1: RabbitMQ will not deliver more than 1 unacked message.
             * - global = false: Applies to this consumer channel specifically.
             * Sends METHOD Frame: Basic.Qos
             */
            channel.basicQos(1);
            System.out.println(" [" + workerName + "] [✓] basicQos(1) ENABLED (Fair Dispatch / Work Stealing Mode)");
        } else {
            System.out.println(" [" + workerName + "] [!] basicQos DISABLED (Blind Round-Robin Starvation Mode)");
        }

        System.out.println(" [" + workerName + "] Delay per order: " + processingDelayMs + "ms");
        System.out.println(" [" + workerName + "] Waiting for orders on '" + QUEUE_NAME + "'. To exit press CTRL+C\n");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            System.out.println(">>> [" + workerName + "] PICKED UP DeliveryTag " + deliveryTag + " -> " + message);

            try {
                // Simulate processing time
                Thread.sleep(processingDelayMs);

                // Send explicit Basic.Ack
                channel.basicAck(deliveryTag, false);
                System.out.println("<<< [" + workerName + "] [COMPLETED & ACKED] DeliveryTag " + deliveryTag);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(" [" + workerName + "] Interrupted!");
            } catch (IOException e) {
                System.err.println(" [" + workerName + "] Error sending ACK: " + e.getMessage());
            }
        };

        boolean autoAck = false; // Always manual ACKs in production!
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {});

        Thread.currentThread().join();
    }
}
