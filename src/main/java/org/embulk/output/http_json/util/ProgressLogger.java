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

    private ProgressLogger() {}

    public static void setSchedule(int loggingInterval) {
        if (service != null) {
            throw new UnsupportedOperationException("already scheduled.");
        }
        if (loggingInterval == 0) {
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

    public static void finish() {
        if (service != null) {
            outputProgress();
            if (!service.isShutdown()) {
                service.shutdown();
            }
        }
    }

    public static void incrementRequestCount() {
        globalRequestCount.incrementAndGet();
    }

    public static void addElapsedTime(long elapsedTIme) {
        globalElapsedTime.addAndGet(elapsedTIme);
    }

    private static void outputProgress() {
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
