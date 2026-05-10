package dev.vishesh.transaction_reconciliation.reconciliation;

import dev.vishesh.transaction_reconciliation.transaction.TransactionType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgTransaction {
    private String id;
    private String transactionId;
    private TransactionType type;
    private String date;
    private String description;
    private BigDecimal amount;
    private String category;
    private String status;
    private boolean isReconciled;
    private LocalDateTime createdAt;
}
