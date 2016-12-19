/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.collector.kafka010;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.StorageComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static zipkin.internal.Util.checkNotNull;

/**
 * This collector polls a Kafka topic for messages that contain TBinaryProtocol big-endian encoded
 * lists of spans. These spans are pushed to a {@link AsyncSpanConsumer#accept span consumer}.
 *
 * <p>This collector remains a Kafka 0.8.x consumer, while Zipkin systems update to 0.9+.
 */
public final class KafkaCollector implements CollectorComponent {

  private static final String DEFAULT_TOPIC = "zipkin";
  private static final String DEFAULT_GROUP_ID = "zipkin";

  final LazyStreams
  final LazyProcessors processors;
  // final LazyStreams streams;

  // NOTE: Any consumer properties updated via setters should take precedence and must update the consumerProperties.
  // See setGroupId() below.
  private Properties consumerProperties;

  KafkaCollector(Builder builder) {
    // Settings below correspond to "New Consumer Configs"
    // http://kafka.apache.org/documentation.html
    Properties props = new Properties();
    props.putAll(builder.consumerProperties);
    props.put("zookeeper.connect", builder.zookeeper);
    props.put("group.id", builder.groupId);
    props.put("fetch.message.max.bytes", String.valueOf(builder.maxMessageSize));
    // Same default as zipkin-scala, and keeps tests from hanging
    props.put("auto.offset.reset", "smallest");
    this.consumerProperties = props;

    connector = new LazyConsumer(builder);
    streams = new LazyStreams(builder, connector);
  }

  @Override public KafkaCollector start() {
    connector.get();
    streams.get();
    return this;
  }

  @Override public CheckResult check() {
    try {
      connector.get(); // make sure the connector didn't throw
      CheckResult failure = streams.failure.get(); // check the streams didn't quit
      if (failure != null) return failure;
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() throws IOException {
    streams.close();
    connector.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Configuration including defaults needed to consume spans from a Kafka topic. */
  public static final class Builder implements CollectorComponent.Builder {
    Collector.Builder delegate = Collector.builder(KafkaCollector.class);
    CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;
    String topic = DEFAULT_TOPIC;
    String zookeeper;
    String groupId = DEFAULT_GROUP_ID;
    Properties consumerProperties = new Properties();
    Deserializer<byte[]> keyDeserializer = new ByteArrayDeserializer();
    Deserializer<byte[]> valueDeserializer = new ByteArrayDeserializer();
    int streams = 1;
    int maxMessageSize = 1024 * 1024;

    @Override public Builder storage(StorageComponent storage) {
      delegate.storage(storage);
      return this;
    }

    @Override public Builder sampler(CollectorSampler sampler) {
      delegate.sampler(sampler);
      return this;
    }

    @Override public Builder metrics(CollectorMetrics metrics) {
      this.metrics = checkNotNull(metrics, "metrics").forTransport("kafka");
      delegate.metrics(this.metrics);
      return this;
    }

    /** Specifies any additional Kafka consumer properties not specified in specific properties in the builder class */
    public Builder properties(Properties consumerProperties) {
      this.consumerProperties = checkNotNull(consumerProperties, "consumerProperties");
      return this;
    }

    /** Topic zipkin spans will be consumed from. Defaults to "zipkin" */
    public Builder topic(String topic) {
      this.topic = checkNotNull(topic, "topic");
      return this;
    }

    /** The zookeeper connect string, ex. 127.0.0.1:2181. No default */
    public Builder zookeeper(String zookeeper) {
      this.zookeeper = checkNotNull(zookeeper, "zookeeper");
      return this;
    }

    /** The consumer group this process is consuming on behalf of. Defaults to "zipkin" */
    public Builder groupId(String groupId) {
      this.groupId = checkNotNull(groupId, "groupId");
      return this;
    }

    /** Count of threads/streams consuming the topic. Defaults to 1 */
    public Builder streams(int streams) {
      this.streams = streams;
      return this;
    }

    /** Maximum size of a message containing spans in bytes. Defaults to 1 MiB */
    public Builder maxMessageSize(int bytes) {
      this.maxMessageSize = bytes;
      return this;
    }

    /** Explicitly provides the Deserializer to use for keys; the default should be sufficient in almost all cases */
    public Builder keyDeserializer(Deserializer<byte[]> keyDeserializer) {
      this.keyDeserializer = checkNotNull(keyDeserializer, "keyDeserializer");
      return this;
    }

    /** Explicitly provides the Deserializer to use for values; the default should be sufficient in almost all cases */
    public Builder valueDeserializer(Deserializer<byte[]> valueDeserializer) {
      this.valueDeserializer = checkNotNull(valueDeserializer, "valueDeserializer");
      return this;
    }

    public KafkaCollector build() {
      return new KafkaCollector(this);
    }

    Builder() {
    }
  }

  static final class LazyStreams extends LazyCloseable<ExecutorService> {
    final int streams;
    final String topic;
    /** the Properties object provided by the developer/configurer of this Collector */
    final Properties providedConsumerProperties;
    /** the "actual" consumer properties that have been augmented with defaults, etc. */
    final Properties consumerProperties;

    final Collector collector;
    final CollectorMetrics metrics;
    final LazyConsumerFactory lazyConsumerFactory;
    final List<LazyCloseable<KafkaConsumer<byte[], byte[]>>> consumerList;
    final AtomicReference<CheckResult> failure = new AtomicReference<>();
    final Deserializer<byte[]> keyDeserializer;
    final Deserializer<byte[]> valueDeserializer;

    LazyStreams(Builder builder) {
      this.keyDeserializer = builder.keyDeserializer;
      this.valueDeserializer = builder.valueDeserializer;
      this.streams = builder.streams;
      this.topic = builder.topic;
      this.collector = builder.delegate.build();
      this.metrics = builder.metrics;
      this.consumerList = new ArrayList<>();
      this.providedConsumerProperties = builder.consumerProperties;

      consumerProperties = new Properties();

      // Settings below correspond to "New Consumer Configs"
      // http://kafka.apache.org/documentation.html
      consumerProperties.putAll(consumerProperties);
      consumerProperties.put("zookeeper.connect", builder.zookeeper);
      consumerProperties.put("group.id", builder.groupId);
      consumerProperties.put("fetch.message.max.bytes", String.valueOf(builder.maxMessageSize));
      // Same default as zipkin-scala, and keeps tests from hanging
      consumerProperties.put("auto.offset.reset", "smallest");
      this.consumerProperties.putAll(builder.consumerProperties);

      this.lazyConsumerFactory = new LazyConsumerFactory(consumerProperties, builder);

    }

    static class ConsumerPool {
      private final ExecutorService executorService;
      private final List<LazyCloseable<KafkaConsumer<byte[], byte[]>>> kafkaConsumers;

      ConsumerPool(ExecutorService executorService, List<LazyCloseable<KafkaConsumer<byte[], byte[]>>> kafkaConsumers) {
        this.executorService = executorService;
        this.kafkaConsumers = kafkaConsumers;

      }
    }

    @Override
    protected ExecutorService compute() {
      final ThreadFactory threadFactory = new ThreadFactory() {
        private final AtomicInteger numThreads = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
          return new Thread(r, String.format("kafka-collector-consumer-%d", numThreads.incrementAndGet()));
        }
      };
      ExecutorService pool = streams == 1 ? Executors.newSingleThreadExecutor(threadFactory) : Executors.newFixedThreadPool(streams,
              threadFactory);
      for (int i = 0; i < streams; i++) {
        LazyCloseable<KafkaConsumer<byte[], byte[]>> lazyConsumer = lazyConsumerFactory.createConsumer(this);

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties, keyDeserializer,
                        valueDeserializer);
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        KafkaConsumerProcessor processor = new KafkaConsumerProcessor(kafkaConsumer, consumerProperties);
        pool.submit(processor);
        consumerList.add(processor);

        KafkaConsumerRunner consumerRunner = new KafkaConsumerRunner<>(kafkaConsumer, consumerProperties,
                messageProcessor, imqKafkaProducer, metricPrefix, kafkaConsumerDependencyChecker);
        executorService.submit(consumerRunner);
        consumerList.add(consumerRunner);
      }

      for (KafkaStream<byte[], byte[]> stream : connector.get().createMessageStreams(topicCountMap)
              .get(topic)) {
        pool.execute(guardFailures(new KafkaStreamProcessor(stream, collector, metrics)));
      }
      return pool;
    }

    Runnable guardFailures(final Runnable delegate) {
      return new Runnable() {
        @Override public void run() {
          try {
            delegate.run();
          } catch (RuntimeException e) {
            failure.set(CheckResult.failed(e));
          }
        }
      };
    }

    @Override
    public void close() {
      ExecutorService maybeNull = maybeNull();
      if (maybeNull != null) {
        maybeNull.shutdown();
      }
    }
  }

  interface ConsumerFactory {

    /**
     * Creates a new lazily initialized KafkaConsumer that optionally reports its subscription/topic-partition
     * assignment and revocation operations to the provided <tt>rebalanceListener</tt>, if provided
     * @param rebalanceListener the optional rebalance listener
     * @return a closeable that when first retrieved will perform the actual consumer initialization
     */
    LazyCloseable<KafkaConsumer<byte[], byte[]>> createConsumer(@Nullable ConsumerRebalanceListener rebalanceListener);
  }

  static final class LazyConsumerFactory implements ConsumerFactory {

    final Properties consumerProperties;
    final String topic;
    final Deserializer<byte[]> keyDeserializer;
    final Deserializer<byte[]> valueDeserializer;

    LazyConsumerFactory(Properties consumerProperties, Builder builder) {
      this.consumerProperties = consumerProperties;
      this.topic = builder.topic;
      this.keyDeserializer = builder.keyDeserializer;
      this.valueDeserializer = builder.valueDeserializer;
    }

    @Override
    public LazyCloseable<KafkaConsumer<byte[], byte[]>> createConsumer(
            @Nullable ConsumerRebalanceListener rebalanceListener) {
      return new LazyConsumer(consumerProperties, topic, keyDeserializer, valueDeserializer, rebalanceListener);
    }
  }

  static final class LazyConsumer extends LazyCloseable<KafkaConsumer<byte[], byte[]>> {

    final Properties consumerProperties;
    final String topic;
    /** optional; if provided, notified of rebalance operations on behalf of the consumer subscribing to this topic */
    @Nullable
    final ConsumerRebalanceListener rebalanceListener;
    final Deserializer<byte[]> keyDeserializer;
    final Deserializer<byte[]> valueDeserializer;

    LazyConsumer(Properties consumerProperties, String topic, Deserializer<byte[]> keyDeserializer,
                 Deserializer<byte[]> valueDeserializer, ConsumerRebalanceListener rebalanceListener) {
      this.consumerProperties = consumerProperties;
      this.topic = topic;
      this.rebalanceListener = rebalanceListener;
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
    }

    @Override
    protected KafkaConsumer<byte[], byte[]> compute() {
      KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties,
              new ByteArrayDeserializer(), new ByteArrayDeserializer());
      if (rebalanceListener != null) {
        kafkaConsumer.subscribe(Collections.singletonList(topic), rebalanceListener);
      } else {
        kafkaConsumer.subscribe(Collections.singletonList(topic));
      }

      return kafkaConsumer;
    }

    @Override
    public void close() {
      KafkaConsumer<byte[], byte[]> maybeNull = maybeNull();
      if (maybeNull != null) {
        maybeNull.close();
      }
    }
  }
}
