package org.embulk.output.http_json.jaxrs;

import java.util.Locale;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSResponseReader;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.slf4j.LoggerFactory;

// NOTE: This file is a copy of the code in the link below, which shows response body on failure.
// https://github.com/embulk/embulk-util-retryhelper/blob/402412d/embulk-util-retryhelper-jaxrs/src/main/java/org/embulk/util/retryhelper/jaxrs/JAXRSRetryHelper.java
public class JAXRSRetryHelper implements AutoCloseable {
    public JAXRSRetryHelper(
            int maximumRetries,
            int initialRetryIntervalMillis,
            int maximumRetryIntervalMillis,
            JAXRSClientCreator clientCreator) {
        this(
                maximumRetries,
                initialRetryIntervalMillis,
                maximumRetryIntervalMillis,
                clientCreator.create(),
                true,
                LoggerFactory.getLogger(JAXRSRetryHelper.class));
    }

    public static JAXRSRetryHelper createWithReadyMadeClient(
            int maximumRetries,
            int initialRetryIntervalMillis,
            int maximumRetryIntervalMillis,
            final javax.ws.rs.client.Client client,
            final org.slf4j.Logger logger) {
        return new JAXRSRetryHelper(
                maximumRetries,
                initialRetryIntervalMillis,
                maximumRetryIntervalMillis,
                client,
                false,
                logger);
    }

    private JAXRSRetryHelper(
            int maximumRetries,
            int initialRetryIntervalMillis,
            int maximumRetryIntervalMillis,
            final javax.ws.rs.client.Client client,
            boolean closeAutomatically,
            final org.slf4j.Logger logger) {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        this.client = client;
        ;
        this.closeAutomatically = closeAutomatically;
        this.logger = logger;
    }

    public <T> T requestWithRetry(
            final JAXRSResponseReader<T> responseReader,
            final JAXRSRequestSuccessCondition successCondition,
            final JAXRSSingleRequester singleRequester) {
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(this.maximumRetries)
                    .withInitialRetryWaitMillis(this.initialRetryIntervalMillis)
                    .withMaxRetryWaitMillis(this.maximumRetryIntervalMillis)
                    .build()
                    .runInterruptible(
                            new Retryable<T>() {
                                @Override
                                public T call() throws Exception {
                                    JAXRSReusableStringResponse response =
                                            new JAXRSReusableStringResponse(
                                                    singleRequester.requestOnce(client));
                                    if (!successCondition.test(response)) {
                                        throw JAXRSWebApplicationExceptionWrapper.wrap(response);
                                    }

                                    return responseReader.readResponse(response);
                                }

                                @Override
                                public boolean isRetryableException(Exception exception) {
                                    return singleRequester.toRetry(exception);
                                }

                                @Override
                                public void onRetry(
                                        Exception exception,
                                        int retryCount,
                                        int retryLimit,
                                        int retryWait)
                                        throws RetryGiveupException {
                                    String message =
                                            String.format(
                                                    Locale.ENGLISH,
                                                    "Retrying %d/%d after %d seconds. Message: %s",
                                                    retryCount,
                                                    retryLimit,
                                                    retryWait / 1000,
                                                    exception.getMessage());
                                    if (retryCount % 3 == 0) {
                                        logger.warn(message, exception);
                                    } else {
                                        logger.warn(message);
                                    }
                                }

                                @Override
                                public void onGiveup(Exception first, Exception last)
                                        throws RetryGiveupException {}
                            });
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        } catch (RetryGiveupException ex) {
            throw new RuntimeException(ex.getCause());
        }
    }

    @Override
    public void close() {
        if (this.closeAutomatically && this.client != null) {
            this.client.close();
        }
    }

    private final int maximumRetries;
    private final int initialRetryIntervalMillis;
    private final int maximumRetryIntervalMillis;
    private final javax.ws.rs.client.Client client;
    private final org.slf4j.Logger logger;
    private final boolean closeAutomatically;
}
