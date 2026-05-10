package dev.vishesh.transaction_reconciliation.fileRecord;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a file upload record stored in the file_records MySQL table.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileRecord {

    private String id;
    private String fileName;
    private String status;
    private int totalRecords;
    private int processedRecords;
    private LocalDateTime uploadedAt;
    private LocalDateTime completedAt;
}
