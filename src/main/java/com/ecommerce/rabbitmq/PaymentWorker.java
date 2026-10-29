package com.ecommerce.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
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
 * LAB 3: PaymentWorker with Fanout Event Broadcasting
 * =========================================================================
 *
 * AMQP MECHANICS: EVENT-DRIVEN DOWNSTREAM BROADCASTING
 *
 * 1. Upon completing a payment, this worker acts as BOTH a Consumer AND a Producer:
 *    - Consumer: Receives order from 'orders.payment' queue.
 *    - Producer: Dispatches 'Payment Completed' event to the Fanout Exchange 'payment.fanout.ex'.
 *
 * 2. Fanout Mechanics:
 *    - channel.basicPublish('payment.fanout.ex', "", null, payload)
 *    - Notice routingKey is passed as empty string "": Fanout exchanges ignore routing keys completely!
 *    - The broker clones the message to both 'email.queue' and 'analytics.queue'.
 */
public class PaymentWorker {

    public static final String PAYMENT_QUEUE = "orders.payment";
    public static final String FANOUT_EXCHANGE = "payment.fanout.ex";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1. Declare Quorum Queue for Orders
        Map<String, Object> queueArgs = new HashMap<>();
        queueArgs.put("x-queue-type", "quorum");
        channel.queueDeclare(PAYMENT_QUEUE, true, false, false, queueArgs);

        // 2. Declare Fanout Exchange for downstream broadcast
        channel.exchangeDeclare(FANOUT_EXCHANGE, BuiltinExchangeType.FANOUT, true, false, null);

        // Fair dispatch
        channel.basicQos(1);

        System.out.println(" [*] PaymentWorker listening on '" + PAYMENT_QUEUE + "'.");
        System.out.println(" [*] Will broadcast completed payments to Fanout Exchange: '" + FANOUT_EXCHANGE + "'\n");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String orderJson = new String(delivery.getBody(), StandardCharsets.UTF_8);
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            System.out.println("\n[PaymentWorker] Processing payment for: " + orderJson);

            try {
                // Simulate payment gateway API processing (1.5s)
                Thread.sleep(1500);

                // Acknowledge the payment order
                channel.basicAck(deliveryTag, false);
                System.out.println("[PaymentWorker] [✓] Payment completed & ACKed (DeliveryTag: " + deliveryTag + ")");

                /*
                 * =============================================================
                 * FANOUT BROADCAST:
                 * Dispatches to 'payment.fanout.ex'. Routing key is ignored.
                 * =============================================================
                 */
                String eventPayload = "{\"event\":\"PAYMENT_SUCCESS\",\"data\":" + orderJson + "}";
                channel.basicPublish(FANOUT_EXCHANGE, "", null, eventPayload.getBytes(StandardCharsets.UTF_8));
                System.out.println("[PaymentWorker] [>>>] Broadcasted event to Fanout Exchange: " + eventPayload);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                System.err.println("[PaymentWorker] Error: " + e.getMessage());
            }
        };

        boolean autoAck = false;
        channel.basicConsume(PAYMENT_QUEUE, autoAck, deliverCallback, consumerTag -> {});

        Thread.currentThread().join();
    }
}
