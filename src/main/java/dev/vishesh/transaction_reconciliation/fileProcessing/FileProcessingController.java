package dev.vishesh.transaction_reconciliation.fileProcessing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import dev.vishesh.transaction_reconciliation.fileRecord.FileRecord;
import dev.vishesh.transaction_reconciliation.fileRecord.FileRecordRepository;
import lombok.RequiredArgsConstructor;

import jakarta.annotation.PostConstruct;

@RestController
@RequestMapping("/file-processing")
@RequiredArgsConstructor
public class FileProcessingController {

    private final FileProcessingService fileProcessingService;
    private final FileRecordRepository fileRecordRepository;

    @Value("${file.upload.temp-dir}")
    private String tempDir;

    @PostConstruct
    public void init() throws IOException {
        // Ensure the temp upload directory exists on startup
        Files.createDirectories(Paths.get(tempDir));
    }

    /**
     * Accepts an XLSX file upload, saves it to disk, queues it for
     * async processing, and returns immediately with a fileId.
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> uploadFile(
            @RequestParam("file") MultipartFile file) throws IOException {

        if (file.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Please upload a non-empty file"));
        }

        String filename = file.getOriginalFilename();
        if (filename == null || !filename.toLowerCase().endsWith(".xlsx")) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Only .xlsx files are supported"));
        }

        // Generate file ID and save to disk
        String fileId = UUID.randomUUID().toString();
        Path filePath = Paths.get(tempDir, fileId + ".xlsx");
        file.transferTo(filePath);

        // Create a QUEUED record in DB
        FileRecord fileRecord = fileProcessingService.queueFile(fileId, filename);

        // Submit for async processing (returns immediately)
        fileProcessingService.processFileAsync(fileId, filename, filePath);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "fileId", fileRecord.getId(),
                        "fileName", fileRecord.getFileName(),
                        "status", fileRecord.getStatus(),
                        "message", "File queued for processing. Poll /file-processing/" + fileId + "/status for progress."
                ));
    }

    /**
     * Returns the current processing status of a file.
     * Clients can poll this endpoint to track progress.
     */
    @GetMapping("/{fileId}/status")
    public ResponseEntity<Map<String, Object>> getFileStatus(
            @PathVariable String fileId) {

        return fileRecordRepository.findById(fileId)
                .map(record -> ResponseEntity.ok(Map.<String, Object>of(
                        "fileId", record.getId(),
                        "fileName", record.getFileName(),
                        "status", record.getStatus(),
                        "processedRecords", record.getProcessedRecords(),
                        "totalRecords", record.getTotalRecords()
                )))
                .orElse(ResponseEntity.notFound().build());
    }
}
