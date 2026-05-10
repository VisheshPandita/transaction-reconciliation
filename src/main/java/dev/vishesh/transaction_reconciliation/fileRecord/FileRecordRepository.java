package dev.vishesh.transaction_reconciliation.fileRecord;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class FileRecordRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Inserts a new file record with the given status.
     */
    public void save(FileRecord fileRecord) {
        jdbcTemplate.update(
                "INSERT INTO file_records (id, file_name, status, total_records, processed_records, uploaded_at) VALUES (?, ?, ?, ?, ?, ?)",
                fileRecord.getId(),
                fileRecord.getFileName(),
                fileRecord.getStatus(),
                fileRecord.getTotalRecords(),
                fileRecord.getProcessedRecords(),
                fileRecord.getUploadedAt());
    }

    /**
     * Finds a file record by ID.
     */
    public Optional<FileRecord> findById(String fileId) {
        return jdbcTemplate.query(
                "SELECT id, file_name, status, total_records, processed_records, uploaded_at, completed_at FROM file_records WHERE id = ?",
                (rs, rowNum) -> FileRecord.builder()
                        .id(rs.getString("id"))
                        .fileName(rs.getString("file_name"))
                        .status(rs.getString("status"))
                        .totalRecords(rs.getInt("total_records"))
                        .processedRecords(rs.getInt("processed_records"))
                        .uploadedAt(rs.getTimestamp("uploaded_at") != null
                                ? rs.getTimestamp("uploaded_at").toLocalDateTime() : null)
                        .completedAt(rs.getTimestamp("completed_at") != null
                                ? rs.getTimestamp("completed_at").toLocalDateTime() : null)
                        .build(),
                fileId).stream().findFirst();
    }

    /**
     * Updates the status to PROCESSING.
     */
    public void markProcessing(String fileId) {
        jdbcTemplate.update(
                "UPDATE file_records SET status = 'PROCESSING' WHERE id = ?",
                fileId);
    }

    /**
     * Updates the processed records count (for progress tracking).
     */
    public void updateProgress(String fileId, int processedRecords) {
        jdbcTemplate.update(
                "UPDATE file_records SET processed_records = ? WHERE id = ?",
                processedRecords,
                fileId);
    }

    /**
     * Marks a file record as COMPLETED with the final record count.
     */
    public void markCompleted(String fileId, int totalRecords) {
        jdbcTemplate.update(
                "UPDATE file_records SET status = 'COMPLETED', total_records = ?, processed_records = ?, completed_at = ? WHERE id = ?",
                totalRecords,
                totalRecords,
                LocalDateTime.now(),
                fileId);
    }

    /**
     * Marks a file record as FAILED.
     */
    public void markFailed(String fileId) {
        jdbcTemplate.update(
                "UPDATE file_records SET status = 'FAILED', completed_at = ? WHERE id = ?",
                LocalDateTime.now(),
                fileId);
    }
}
