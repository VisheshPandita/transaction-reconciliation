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
public class ReconciledTransaction {
    private String id;
    private String transactionId;
    private TransactionType type;
    private String pgTransactionId;
    private String clientFileId;
    private String clientTransactionId;
    private String reconciliationStatus;
    private BigDecimal pgAmount;
    private BigDecimal clientAmount;
    private LocalDateTime reconciledAt;
}
