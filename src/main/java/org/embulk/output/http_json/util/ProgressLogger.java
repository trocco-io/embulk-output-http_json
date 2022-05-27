package org.embulk.output.http_json.util;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jersey.repackaged.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressLogger {
    private static final long INITIAL_DELAY_SECONDS = 1;

    private static final Logger logger = LoggerFactory.getLogger(ProgressLogger.class);

    private final ScheduledExecutorService service;

    private final AtomicLong globalRequestCount = new AtomicLong(0);
    private final AtomicLong globalElapsedTime = new AtomicLong(0);

    public ProgressLogger(Duration loggingInterval) {
        service =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat(ProgressLogger.class.getSimpleName())
                                .build());
        long loggingIntervalSecond = loggingInterval.getSeconds();
        if (loggingIntervalSecond > 0) {
            setSchedule(loggingIntervalSecond);
        } else {
            logger.warn("disabled progress log.");
            service.shutdown();
        }
    }

    private void setSchedule(long loggingInterval) {
        service.scheduleAtFixedRate(
                () -> outputProgress(), INITIAL_DELAY_SECONDS, loggingInterval, TimeUnit.SECONDS);
    }

    public void finish() {
        if (!service.isShutdown()) {
            service.submit(() -> outputProgress());
            service.shutdown();
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
