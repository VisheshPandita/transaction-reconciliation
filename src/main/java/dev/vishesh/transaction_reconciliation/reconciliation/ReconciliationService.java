package dev.vishesh.transaction_reconciliation.reconciliation;

import dev.vishesh.transaction_reconciliation.transaction.Transaction;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final ReconciliationRepository reconciliationRepository;

    /**
     * Processes a batch of TransactionBatchMessage from Kafka.
     * Extracts all transactions, does a bulk fetch from the DB, matches them in
     * memory,
     * and performs a bulk insert of the results.
     */
    public void processBatch(List<Transaction> incomingTransactions) {
        if (incomingTransactions == null || incomingTransactions.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();

        // 1. Extract all transaction IDs to fetch
        Set<String> transactionIdsToFetch = new HashSet<>();
        for (Transaction txn : incomingTransactions) {
            transactionIdsToFetch.add(txn.getTransactionId());
        }

        // 2. Bulk fetch PG transactions from DB
        List<PgTransaction> pgTransactions = reconciliationRepository.findByTransactionIds(transactionIdsToFetch);

        // 3. Group PG transactions by transactionId + type for O(1) in-memory lookup
        Map<String, List<PgTransaction>> pgTxnMap = new HashMap<>();
        for (PgTransaction pgTxn : pgTransactions) {
            String key = pgTxn.getTransactionId() + "_" + pgTxn.getType().name();
            pgTxnMap.computeIfAbsent(key, k -> new ArrayList<>(2)).add(pgTxn);
        }

        // 4. Perform In-Memory Matching
        List<ReconciledTransaction> reconciledResults = new ArrayList<>(incomingTransactions.size());
        List<String> pgTransactionIdsToMarkAsReconciled = new ArrayList<>();

        for (Transaction clientTxn : incomingTransactions) {
            String key = clientTxn.getTransactionId() + "_" + clientTxn.getType().name();

            List<PgTransaction> potentialMatches = pgTxnMap.get(key);

            ReconciledTransaction.ReconciledTransactionBuilder builder = ReconciledTransaction.builder()
                    .id(UUID.randomUUID().toString())
                    .transactionId(clientTxn.getTransactionId())
                    .type(clientTxn.getType())
                    .clientFileId(clientTxn.getFileId())
                    .clientTransactionId(clientTxn.getId())
                    .clientAmount(clientTxn.getAmount());

            if (potentialMatches != null && !potentialMatches.isEmpty()) {
                // Try to find a PG transaction with the exact matching amount first
                // (handles out-of-order partial refunds)
                PgTransaction matchedPg = null;
                for (PgTransaction pg : potentialMatches) {
                    if (clientTxn.getAmount() != null && clientTxn.getAmount().compareTo(pg.getAmount()) == 0) {
                        matchedPg = pg;
                        break;
                    }
                }

                if (matchedPg != null) {
                    // We found an exact amount match
                    potentialMatches.remove(matchedPg);
                    builder.pgTransactionId(matchedPg.getId())
                            .pgAmount(matchedPg.getAmount())
                            .reconciliationStatus("MATCHED");
                    pgTransactionIdsToMarkAsReconciled.add(matchedPg.getId());
                } else {
                    // No exact amount match found, just take the first available one to record the mismatch
                    matchedPg = potentialMatches.remove(0);
                    builder.pgTransactionId(matchedPg.getId())
                            .pgAmount(matchedPg.getAmount())
                            .reconciliationStatus("AMOUNT_MISMATCH");
                    // We DO NOT add it to pgTransactionIdsToMarkAsReconciled, leaving it available in PG table
                }
            } else {
                // Not found in our unreconciled PG records
                builder.reconciliationStatus("MISSING_IN_PG");
            }

            reconciledResults.add(builder.build());
        }

        // 5. Bulk update PG transactions to mark them as reconciled
        reconciliationRepository.batchUpdatePgTransactionsAsReconciled(pgTransactionIdsToMarkAsReconciled);

        // 6. Bulk Insert the results
        reconciliationRepository.batchInsertReconciledTransactions(reconciledResults);

        long duration = System.currentTimeMillis() - startTime;
        log.info("Reconciled batch of {} transactions in {} ms", incomingTransactions.size(), duration);
    }

}
