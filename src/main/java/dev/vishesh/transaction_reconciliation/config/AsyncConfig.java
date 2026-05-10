package dev.vishesh.transaction_reconciliation.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configures async file processing with a bounded thread pool.
 * Pool parameters are configurable via application.properties.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    @Value("${file.processing.pool.core-size:5}")
    private int corePoolSize;

    @Value("${file.processing.pool.max-size:5}")
    private int maxPoolSize;

    @Value("${file.processing.pool.queue-capacity:500}")
    private int queueCapacity;

    @Bean(name = "fileProcessingExecutor")
    public Executor fileProcessingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("file-processor-");
        executor.setRejectedExecutionHandler((runnable, exec) -> {
            throw new RuntimeException(
                    "File processing queue is full. Please try again later.");
        });
        executor.initialize();
        return executor;
    }
}
