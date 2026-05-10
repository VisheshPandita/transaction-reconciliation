package dev.vishesh.transaction_reconciliation.transaction;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    private String id;
    private String fileId;
    private String transactionId;
    private TransactionType type;
    private String date;
    private String description;
    private BigDecimal amount;
    private String category;
    private String status;
}
