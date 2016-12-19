package zipkin.collector.kafka010;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.internal.Lazy;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Message processor class that is run in a background thread thru an ExecutorService for each thread that the consumer
 * is configured to consume on.
 *
 * @author dgarson
 */
public class KafkaConsumerProcessor implements Runnable {

    protected static final String CONSUMER_POLL_TIMEOUT_PROPERTY = "poll.timeout.ms";
    private static final String DEFAULT_CONSUMER_POLL_TIMEOUT = "1000"; // 1 second

    protected static final String ENABLE_BATCHING = "enable.batching";
    private static final String DEFAULT_ENABLE_BATCHING = "false";

    protected static final String CONSUMER_PROPERTY_GROUP_ID = "group.id";
    protected static final String METRICS_PREFIX_PROPERTY = "metrics.prefix";

    private static final String DEFAULT_AUTO_COMMIT_CONFIG = "true";

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerProcessor.class);

    private final LazyCloseable<KafkaConsumer<byte[], byte[]>> kafkaConsumer;
    private final Properties consumerProperties;

    private final long pollTimeoutMillis;
    private final boolean batchingEnabled;
    private final boolean autoCommitEnabled;
    private final Lazy<KafkaConsumerProcessorMetrics> metrics;

    KafkaConsumerProcessor(final LazyCloseable<KafkaConsumer<byte[], byte[]>> kafkaConsumer,
                           Properties consumerProperties) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerProperties = consumerProperties;

        this.pollTimeoutMillis = Long.parseLong(consumerProperties.getProperty(CONSUMER_POLL_TIMEOUT_PROPERTY,
                DEFAULT_CONSUMER_POLL_TIMEOUT));
        this.batchingEnabled = Boolean.parseBoolean(consumerProperties.getProperty(ENABLE_BATCHING,
                DEFAULT_ENABLE_BATCHING));
        this.autoCommitEnabled = Boolean.parseBoolean(
                consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_AUTO_COMMIT_CONFIG));

        // make sure to defer construction of the KafkaConsumerProcessorMetrics to avoid eagerly connecting to Kafka
        //      and eliminating any optimization introduced by using LazyCloseable's
        final String metricPrefix = consumerProperties.getProperty(METRICS_PREFIX_PROPERTY);
        this.metrics = new Lazy<KafkaConsumerProcessorMetrics>() {
            @Override
            public KafkaConsumerProcessorMetrics compute() {
                return KafkaConsumerProcessorMetrics.forKafkaConsumer(kafkaConsumer.get(),
                        metricPrefix, batchingEnabled);
            }
        };
    }

    protected void processMessages(List<byte[]> messages, OffsetCommitter offsetCommitter) {

    }

    protected void processMessage(byte[] messageBytes, @Nullable Map<TopicPartition, OffsetAndMetadata> commitOffset,
                                  OffsetCommitter offsetCommitter) {

    }

    protected void processMessage(byte[] messageBytes) {
        processMessage(messageBytes, /*commitOffsets=*/null, /*offsetCommitter=*/null);
    }

    @Override
    public void run() {
        final String threadName = Thread.currentThread().getName();
        final OffsetCommitter offsetCommitter = OffsetCommitter.Factory.committer(kafkaConsumer.get(),
                // use async commits if we are batching consumed messages(?)
                autoCommitEnabled, /*isAsync=*/batchingEnabled);
        try {
            logger.debug("Iterate through messages in Thread={}", threadName);
            while (true) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.get().poll(pollTimeoutMillis);
                if (records.isEmpty()) {
                    continue;
                }

                if (batchingEnabled) {
                    logger.debug("Processing messages for Thread={} in batch size of={}",
                            threadName, records.count());
                    List<byte[]> messages = new ArrayList<>();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        messages.add(record.value());
                    }
                    long startTimeMillis = System.currentTimeMillis();
                    try {
                        processMessages(messages, offsetCommitter);
                    } catch (Throwable throwable) {
                        logger.error("An error occurred when batch processing messages={}", messages);
                        onProcessingError(messages, throwable);
                    } finally {
                        long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
                        onProcessingCompleted(elapsedMillis);
                    }
                } else {
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        logger.debug("Processing message for Thread={}, Partition={}, Topic={} and Offset={}",
                                threadName, record.partition(), record.topic(), record.offset());
                        final byte[] message = record.value();
                        long startTimeMillis = System.currentTimeMillis();
                        try {
                            if (autoCommitEnabled) {
                                processMessage(message);
                            } else {
                                processMessage(message, getCommitOffset(record), offsetCommitter);
                            }
                        } catch (WakeupException e) {
                            // KafkaOffsetsCommitter throws WakeupException since it's using ConsumerNetworkClient.poll
                            // internally, catch and re-throw the exception so that it can be handled properly below.
                            throw e;
                        } catch (Throwable throwable) {
                            onProcessingError(Collections.singletonList(message), throwable);
                        } finally {
                            long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
                            onProcessingCompleted(elapsedMillis);
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // WakeupException is caught when calling kafkaConsumer.wakeup()
            logger.info("Shutting down kafka consumer thread={}", threadName, e);
        } catch (Throwable throwable) {
            // not confident that we seen all errors in the logs, to be safe logger errors here.
            logger.error("Unhandled error in kafka consumer thread.", throwable);
        } finally {
            // if auto-commit is enabled, close() will commit the current offsets.
            try {
                kafkaConsumer.close();
            } catch (Exception e) {
                logger.error("Unable to shutdown parent consumer", e);
            }
        }
    }

    /**
     * Disconnect kafkaConsumer from Kafka brokers.
     */
    public void shutdown() {
        // wakeup() will trigger a WakeupException in {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long)}
        // and interrupt KafkaConsumer's active operation
        kafkaConsumer.get().wakeup();
    }

    private void onProcessingError(List<byte[]> messages, Throwable cause) {
        metrics.get().recordTotalError();
        // We always record here. Otherwise it may be too late to do it after the switch block because
        // some cases re-throw the "throwable".
        metrics.get().recordThrowable(cause);
    }

    private void onProcessingCompleted(long elapsedMillis) {

    }

    /**
     * Create a map containing offset info given a ConsumerRecord object.
     */
    private Map<TopicPartition, OffsetAndMetadata> getCommitOffset(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        //The committed offset should be the next message your application will consume.
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, null);
        return Collections.singletonMap(topicPartition, offsetAndMetadata);
    }

    /**
     * Get the internal "metrics" field of the given KafkaConsumer instance. This allows us to use
     * the metrics system used by Kafka itself.
     * @throws RuntimeException if KafkaConsumer no longer has this field.
     */
    public static Metrics getInternalMetrics(KafkaConsumer<?, ?> instance) {
        return getMetrics(instance);
    }

    @SuppressWarnings("unchecked")
    private static Metrics getMetrics(Object instance) {
        try {
            Field metricsField = instance.getClass().getDeclaredField("metrics");
            metricsField.setAccessible(true);
            return (Metrics) metricsField.get(instance);
        } catch (NoSuchFieldException nsme) {
            throw new RuntimeException("Unable to locate \"metrics\" field in " + instance.getClass(), nsme);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Unable to retrieve value for \"metrics\" field in " + instance.getClass(), iae);
        }
    }
}
