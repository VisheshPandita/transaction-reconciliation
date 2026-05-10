CREATE TABLE IF NOT EXISTS file_records (
    id VARCHAR(36) PRIMARY KEY,
    file_name VARCHAR(500) NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_records INT DEFAULT 0,
    processed_records INT DEFAULT 0,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL
);

-- Transactions recorded by us (the Payment Gateway)
CREATE TABLE IF NOT EXISTS pg_transactions (
    id VARCHAR(36) PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL, -- 'CAPTURE' or 'REFUND'
    date VARCHAR(50),
    description VARCHAR(500),
    amount DECIMAL(15, 2) NOT NULL,
    category VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    is_reconciled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Results of the reconciliation process between Kafka incoming data and pg_transactions
CREATE TABLE IF NOT EXISTS reconciled_transactions (
    id VARCHAR(36) PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL,
    pg_transaction_id VARCHAR(36),      -- Link to our PG record (if found)
    client_file_id VARCHAR(36),         -- Link to the uploaded file record
    client_transaction_id VARCHAR(36),  -- The UUID of the incoming transaction
    reconciliation_status VARCHAR(50) NOT NULL, -- e.g., 'MATCHED', 'AMOUNT_MISMATCH', 'MISSING_IN_PG'
    pg_amount DECIMAL(15, 2),
    client_amount DECIMAL(15, 2),
    reconciled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for ultra-fast bulk lookups during reconciliation
CREATE INDEX idx_pg_txn_type ON pg_transactions(transaction_id, type);
