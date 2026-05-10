package dev.vishesh.transaction_reconciliation.reconciliation;

import dev.vishesh.transaction_reconciliation.transaction.Transaction;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ReconciliationKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationKafkaConsumer.class);

    private final ReconciliationService reconciliationService;

    /**
     * Batch listener for the 'reconciliation' topic.
     * Consumes multiple Transactions at once (up to max.poll.records)
     * and delegates to the service for bulk processing.
     */
    @KafkaListener(
            topics = "reconciliation",
            groupId = "${spring.kafka.consumer.group-id:reconciliation-group}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${spring.kafka.listener.concurrency:20}"
    )
    public void consumeBatches(List<Transaction> messages) {
        log.debug("Received {} batch messages from Kafka", messages.size());
        
        try {
            reconciliationService.processBatch(messages);
        } catch (Exception e) {
            log.error("Failed to process reconciliation batch: {}", e.getMessage(), e);
            // Depending on requirements, you might want to throw the exception to trigger Kafka retry
            // or send to a Dead Letter Queue. For now, we catch and log to prevent consumer crash.
            throw e; 
        }
    }
}
