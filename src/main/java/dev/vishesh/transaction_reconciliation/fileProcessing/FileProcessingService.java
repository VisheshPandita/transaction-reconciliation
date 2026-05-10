package dev.vishesh.transaction_reconciliation.fileProcessing;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import dev.vishesh.transaction_reconciliation.fileRecord.FileRecord;
import dev.vishesh.transaction_reconciliation.fileRecord.FileRecordRepository;
import dev.vishesh.transaction_reconciliation.transaction.Transaction;
import dev.vishesh.transaction_reconciliation.transaction.TransactionKafkaProducer;
import dev.vishesh.transaction_reconciliation.transaction.TransactionType;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class FileProcessingService {

    private static final Logger log = LoggerFactory.getLogger(FileProcessingService.class);
    private static final int BATCH_SIZE = 1000;
    private static final int PROGRESS_UPDATE_INTERVAL = 10_000;

    private final FileRecordRepository fileRecordRepository;
    private final TransactionKafkaProducer kafkaProducer;

    /**
     * Creates a file record with QUEUED status and returns it immediately.
     * The actual processing happens asynchronously via {@link #processFileAsync}.
     *
     * @param fileId   the pre-generated file ID
     * @param fileName the original file name
     * @return the FileRecord in QUEUED status
     */
    public FileRecord queueFile(String fileId, String fileName) {
        FileRecord fileRecord = FileRecord.builder()
                .id(fileId)
                .fileName(fileName)
                .status("QUEUED")
                .totalRecords(0)
                .processedRecords(0)
                .uploadedAt(LocalDateTime.now())
                .build();
        fileRecordRepository.save(fileRecord);
        return fileRecord;
    }

    /**
     * Asynchronously processes an XLSX file from disk:
     * 1. Updates status to PROCESSING
     * 2. Streams the XLSX row-by-row using FastExcel
     * 3. Sends transactions to Kafka in batches of 1000
     * 4. Updates progress every 10,000 records
     * 5. Updates status to COMPLETED (or FAILED on error)
     * 6. Deletes the temp file after processing
     *
     * @param fileId   the file record ID
     * @param fileName the original file name
     * @param filePath path to the temp file on disk
     */
    @Async("fileProcessingExecutor")
    public void processFileAsync(String fileId, String fileName, Path filePath) {
        log.info("Starting async processing for file {} ({})", fileId, fileName);
        fileRecordRepository.markProcessing(fileId);

        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicInteger batchNumber = new AtomicInteger(0);

        try (InputStream inputStream = new FileInputStream(filePath.toFile());
             ReadableWorkbook workbook = new ReadableWorkbook(inputStream)) {

            Sheet sheet = workbook.getFirstSheet();
            List<Transaction> batch = new ArrayList<>(BATCH_SIZE);

            // Stream rows one at a time — entire file is never loaded into memory
            sheet.openStream().forEach(row -> {
                // Skip the header row (row numbers are 1-based in FastExcel)
                if (row.getRowNum() == 1) {
                    return;
                }

                batch.add(mapRowToTransaction(row, fileId));

                if (batch.size() >= BATCH_SIZE) {
                    batch.forEach(kafkaProducer::sendTransaction);
                    int currentTotal = totalCount.addAndGet(batch.size());
                    batch.clear();

                    // Update progress in DB every PROGRESS_UPDATE_INTERVAL records
                    if (currentTotal % PROGRESS_UPDATE_INTERVAL == 0) {
                        fileRecordRepository.updateProgress(fileId, currentTotal);
                        log.info("File {}: {} records sent to Kafka", fileId, currentTotal);
                    }
                }
            });

            // Flush remaining records in the last partial batch
            if (!batch.isEmpty()) {
                batch.forEach(kafkaProducer::sendTransaction);
                totalCount.addAndGet(batch.size());
            }

            // Mark file as completed
            fileRecordRepository.markCompleted(fileId, totalCount.get());
            log.info("File processing complete. File: {}, Total records sent to Kafka: {}",
                    fileId, totalCount.get());

        } catch (Exception e) {
            log.error("Failed to process file {}: {}", fileId, e.getMessage(), e);
            fileRecordRepository.markFailed(fileId);
        } finally {
            // Clean up the temp file
            try {
                Files.deleteIfExists(filePath);
                log.debug("Deleted temp file: {}", filePath);
            } catch (IOException e) {
                log.warn("Failed to delete temp file {}: {}", filePath, e.getMessage());
            }
        }
    }

    // sendBatchToKafka method removed as we now send individual transactions

    /**
     * Maps a FastExcel Row to a Transaction object.
     * Each row gets a unique UUID as its id.
     * Adjust column indices (0-based) to match your actual XLSX structure.
     */
    private Transaction mapRowToTransaction(Row row, String fileId) {
        return Transaction.builder()
                .id(UUID.randomUUID().toString())
                .fileId(fileId)
                .transactionId(getCellValue(row, 0))
                .type(parseTransactionType(getCellValue(row, 1)))
                .date(getCellValue(row, 2))
                .description(getCellValue(row, 3))
                .amount(parseAmount(getCellValue(row, 4)))
                .category(getCellValue(row, 5))
                .status(getCellValue(row, 6))
                .build();
    }

    /**
     * Safely reads a cell value as a String, returning null if the cell is empty.
     */
    private String getCellValue(Row row, int cellIndex) {
        try {
            return row.getCellAsString(cellIndex).orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses a string to TransactionType enum. Defaults to CAPTURE if unrecognized.
     */
    private TransactionType parseTransactionType(String value) {
        if (value == null || value.isBlank()) {
            return TransactionType.CAPTURE;
        }
        try {
            return TransactionType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Unknown transaction type '{}', defaulting to CAPTURE", value);
            return TransactionType.CAPTURE;
        }
    }

    /**
     * Parses a string amount to BigDecimal, returning null if invalid.
     */
    private BigDecimal parseAmount(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Could not parse amount: '{}'", value);
            return null;
        }
    }
}
