package zipkin.collector.kafka010;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.internal.Nullable;

import java.util.Map;

/**
 * Abstraction layer on top of either asynchronous or synchronous committing of offsets to Kafka when polling and/or
 * consuming messages.
 */
abstract class OffsetCommitter {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final boolean autoCommitEnabled;
    protected final KafkaConsumer<?, ?> consumer;

    protected OffsetCommitter(KafkaConsumer<?, ?> consumer, boolean autoCommitEnabled) {
        this.consumer = consumer;
        this.autoCommitEnabled = autoCommitEnabled;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed list of topics and partitions.
     * @see #doCommitOffsets(Map)
     */
    public final void commitOffsets() {
        if (autoCommitEnabled) {
            logger.warn("Cannot commit offsets because consumers are configured to commit" +
                    " offsets automatically!");
            return;
        }
        doCommitOffsets(/*offsets=*/null);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * @param offsets the offsets for each topic to update
     * @see #doCommitOffsets(Map)
     */
    public final void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (autoCommitEnabled) {
            logger.warn("Cannot commit offsets because consumers are configured to commit" +
                    " offsets automatically!");
            return;
        }
        doCommitOffsets(offsets);
    }

    /**
     * Implementations of this method perform the "actual" commit of the offsets. The parameter value will be
     * <code>null</code> if using the zero-argument method {@link #commitOffsets()}.
     * @param offsets the offset to commit per topic partition, or null to automatically commit offsets from the
     *                      most recent poll
     */
    protected abstract void doCommitOffsets(@Nullable Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * Simple Factory for creating new committer instances based on whether autoCommit is enabled or not.
     */
    public static class Factory {

        /**
         * Creates the appropriate OffsetCommitter given the owning consumer and indicators as to whether autoCommit is
         * enabled and whether offset commits should be made asynchronously vs. synchronously.
         */
        public static OffsetCommitter committer(KafkaConsumer<?, ?> consumer, boolean autoCommitEnabled,
                                                boolean isAsync) {
            return isAsync ? new AsyncOffsetCommitter(consumer, autoCommitEnabled) :
                    new SyncOffsetCommitter(consumer, autoCommitEnabled);
        }
    }


    private static class AsyncOffsetCommitter extends OffsetCommitter {

        AsyncOffsetCommitter(KafkaConsumer<?, ?> consumer, boolean autoCommitEnabled) {
            super(consumer, autoCommitEnabled);
        }

        @Override
        protected void doCommitOffsets(@Nullable Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (offsets != null) {
                consumer.commitSync(offsets);
            } else {
                consumer.commitSync();
            }
        }
    }

    private static class SyncOffsetCommitter extends OffsetCommitter {

        SyncOffsetCommitter(KafkaConsumer<?, ?> consumer, boolean autoCommitEnabled) {
            super(consumer, autoCommitEnabled);
        }

        @Override
        protected void doCommitOffsets(@Nullable Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (offsets != null) {
                consumer.commitAsync(offsets, null);
            } else {
                consumer.commitAsync();
            }
        }
    }
}
