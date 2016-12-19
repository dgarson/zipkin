package zipkin.collector.kafka010;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Optional listener that can be attached to a KafkaConsumer such that all rebalance operations and partition
 * revocations or re-assignments are logged.
 */
public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // no-op by default
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // no-op by default
    }
}
