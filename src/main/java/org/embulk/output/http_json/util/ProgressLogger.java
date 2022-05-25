package org.embulk.output.http_json.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jersey.repackaged.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressLogger {
    private static final long INITIAL_DELAY_SECONDS = 1;
    private static AtomicLong globalRequestCount = new AtomicLong(0);
    private static AtomicLong globalElapsedTime = new AtomicLong(0);

    private static final Logger logger = LoggerFactory.getLogger(ProgressLogger.class);

    private static ScheduledExecutorService service;

    private final int loggingInterval;

    public ProgressLogger(int loggingInterval) {
        this.loggingInterval = loggingInterval;
    }

    public void initializeLogger() {
        if (service != null) {
            throw new UnsupportedOperationException("already initialized.");
        }
        if (loggingInterval == 0) {
            logger.warn("disabled progress log.");
            return;
        }
        ThreadFactory factory =
                new ThreadFactoryBuilder()
                        .setNameFormat(ProgressLogger.class.getSimpleName())
                        .build();
        service = Executors.newSingleThreadScheduledExecutor(factory);
        service.scheduleAtFixedRate(
                () -> {
                    outputProgress();
                },
                INITIAL_DELAY_SECONDS,
                loggingInterval,
                TimeUnit.SECONDS);
    }

    public void finish() {
        if (service != null) {
            outputProgress();
            if (!service.isShutdown()) {
                service.shutdown();
            }
        }
    }

    public void incrementRequestCount() {
        globalRequestCount.incrementAndGet();
    }

    public void addElapsedTime(long elapsedTIme) {
        globalElapsedTime.addAndGet(elapsedTIme);
    }

    private void outputProgress() {
        long requestCount = globalRequestCount.get();
        long elapsedTime = globalElapsedTime.get();
        if (requestCount == 0) {
            return;
        }
        logger.info(
                "request count: {}, response time avg: {} ms",
                requestCount,
                elapsedTime / requestCount);
    }
}
