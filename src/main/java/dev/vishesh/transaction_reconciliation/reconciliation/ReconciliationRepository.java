package dev.vishesh.transaction_reconciliation.reconciliation;

import dev.vishesh.transaction_reconciliation.transaction.TransactionType;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class ReconciliationRepository {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    /**
     * Highly optimized bulk fetch using WHERE IN.
     * Fetches all PG transactions that match the given transaction_ids and are NOT yet reconciled.
     */
    public List<PgTransaction> findByTransactionIds(Set<String> transactionIds) {
        if (transactionIds == null || transactionIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT id, transaction_id, type, date, description, amount, category, status, is_reconciled, created_at " +
                     "FROM pg_transactions WHERE transaction_id IN (:ids) AND is_reconciled = FALSE";

        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("ids", transactionIds);

        return namedParameterJdbcTemplate.query(sql, parameters, (rs, rowNum) ->
                PgTransaction.builder()
                        .id(rs.getString("id"))
                        .transactionId(rs.getString("transaction_id"))
                        .type(TransactionType.valueOf(rs.getString("type")))
                        .date(rs.getString("date"))
                        .description(rs.getString("description"))
                        .amount(rs.getBigDecimal("amount"))
                        .category(rs.getString("category"))
                        .status(rs.getString("status"))
                        .isReconciled(rs.getBoolean("is_reconciled"))
                        .createdAt(rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toLocalDateTime() : null)
                        .build()
        );
    }

    /**
     * Ultra-fast bulk insert for reconciled transactions.
     * Uses JdbcTemplate.batchUpdate to execute in a single network round-trip.
     */
    public void batchInsertReconciledTransactions(List<ReconciledTransaction> reconciledTransactions) {
        if (reconciledTransactions == null || reconciledTransactions.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO reconciled_transactions " +
                     "(id, transaction_id, type, pg_transaction_id, client_file_id, client_transaction_id, reconciliation_status, pg_amount, client_amount) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ReconciledTransaction rt = reconciledTransactions.get(i);
                ps.setString(1, rt.getId());
                ps.setString(2, rt.getTransactionId());
                ps.setString(3, rt.getType().name());
                ps.setString(4, rt.getPgTransactionId());
                ps.setString(5, rt.getClientFileId());
                ps.setString(6, rt.getClientTransactionId());
                ps.setString(7, rt.getReconciliationStatus());
                ps.setBigDecimal(8, rt.getPgAmount());
                ps.setBigDecimal(9, rt.getClientAmount());
            }

            @Override
            public int getBatchSize() {
                return reconciledTransactions.size();
            }
        });
    }

    /**
     * Updates the status of the PG transactions to mark them as reconciled.
     */
    public void batchUpdatePgTransactionsAsReconciled(List<String> pgTransactionIds) {
        if (pgTransactionIds == null || pgTransactionIds.isEmpty()) {
            return;
        }

        String sql = "UPDATE pg_transactions SET is_reconciled = TRUE WHERE id IN (:ids)";

        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("ids", pgTransactionIds);

        namedParameterJdbcTemplate.update(sql, parameters);
    }
}
