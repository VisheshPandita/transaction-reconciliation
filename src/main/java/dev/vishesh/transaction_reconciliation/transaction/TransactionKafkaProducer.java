package dev.vishesh.transaction_reconciliation.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class TransactionKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionKafkaProducer.class);
    private static final String TOPIC = "reconciliation";

    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    /**
     * Sends an individual transaction to Kafka.
     * The Kafka message KEY is the transactionId. This guarantees that all transactions
     * (captures and multiple refunds) with the same transactionId go to the same Kafka partition,
     * which prevents race conditions during reconciliation.
     */
    public void sendTransaction(Transaction transaction) {
        kafkaTemplate.send(TOPIC, transaction.getTransactionId(), transaction);
    }
}
