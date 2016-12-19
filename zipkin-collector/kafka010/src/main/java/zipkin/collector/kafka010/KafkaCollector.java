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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.StorageComponent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  private static final Logger logger = LoggerFactory.getLogger(KafkaCollector.class);

  private static final String DEFAULT_TOPIC = "zipkin";
  private static final String DEFAULT_GROUP_ID = "zipkin";

  final LazyConsumers consumers;


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

    consumers = new LazyConsumers(builder, props);
  }

  @Override public KafkaCollector start() {
    consumers.get();
    return this;
  }

  @Override public CheckResult check() {
    try {
      consumers.get(); // make sure the connect-and-subscribe didn't throw
      CheckResult failure = consumers.failure.get(); // check the consumer(s) didn't quit
      if (failure != null) {
        return failure;
      }
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() throws IOException {
    consumers.close();
  }

  static class ConsumerPool implements Closeable {
    private final ExecutorService executorService;
    private final List<LazyConsumer> kafkaConsumers;

    ConsumerPool(ExecutorService executorService, List<LazyConsumer> kafkaConsumers) {
      this.executorService = executorService;
      this.kafkaConsumers = kafkaConsumers;
    }

    @Override
    public void close() throws IOException {
      // schedule shutdown of executor
      executorService.shutdown();

      // close each explicitly so as to force each consumer to commit and abort any polling it may be doing
      for (LazyConsumer consumer : kafkaConsumers) {
        // TODO(dgarson): should this be a wakeup() or close() ?
        consumer.close();
      }
      kafkaConsumers.clear();
    }
  }

  static final class LazyConsumers extends LazyCloseable<ConsumerPool> {
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

    LazyConsumers(Builder builder, Properties consumerProperties) {
      this.keyDeserializer = builder.keyDeserializer;
      this.valueDeserializer = builder.valueDeserializer;
      this.streams = builder.streams;
      this.topic = builder.topic;
      this.collector = builder.delegate.build();
      this.metrics = builder.metrics;
      this.consumerList = new ArrayList<>();

      // copy the original consumer properties provided to the builder
      this.providedConsumerProperties = new Properties();
      this.providedConsumerProperties.putAll(builder.consumerProperties);
      this.consumerProperties = consumerProperties;

      consumerProperties = new Properties();

      // Settings below correspond to "New Consumer Configs"
      // http://kafka.apache.org/documentation.html
      consumerProperties.put("zookeeper.connect", builder.zookeeper);
      consumerProperties.put("group.id", builder.groupId);
      consumerProperties.put("fetch.message.max.bytes", String.valueOf(builder.maxMessageSize));
      // Same default as zipkin-scala, and keeps tests from hanging
      consumerProperties.put("auto.offset.reset", "smallest");
      this.consumerProperties.putAll(builder.consumerProperties);

      this.lazyConsumerFactory = new LazyConsumerFactory(consumerProperties, builder);
    }

    @Override
    protected ConsumerPool compute() {
      final ThreadFactory threadFactory = new ThreadFactory() {
        private final AtomicInteger numThreads = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
          return new Thread(r, String.format("kafka-collector-consumer-%d", numThreads.incrementAndGet()));
        }
      };
      ExecutorService executorService = streams == 1 ? Executors.newSingleThreadExecutor(threadFactory) :
              Executors.newFixedThreadPool(streams, threadFactory);
      List<LazyConsumer> consumerList = new ArrayList<>();
      for (int i = 0; i < streams; i++) {
        // create and start the consumer -- it should be subscribed to this topic prior returning a new instance
        LazyConsumer consumer = lazyConsumerFactory.createConsumer();
        consumerList.add(consumer);
        executorService.submit(guardFailures(new KafkaConsumerProcessor(consumer, consumerProperties)));
      }
      return new ConsumerPool(executorService, consumerList);
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
  }

  interface ConsumerFactory {

    /**
     * Creates a new lazily initialized KafkaConsumer that will be constructed and subscribe to its assigned topic only
     * after first attempted usage.
     * @return a closeable that when first retrieved will perform the actual consumer initialization
     */
    LazyConsumer createConsumer();
  }

  static final class LazyConsumerFactory implements ConsumerFactory {

    final Properties consumerProperties;
    final String topic;
    final Deserializer<byte[]> keyDeserializer;
    final Deserializer<byte[]> valueDeserializer;
    final ConsumerRebalanceListener rebalanceListener;

    LazyConsumerFactory(Properties consumerProperties, KafkaCollector.Builder builder) {
      this.consumerProperties = consumerProperties;
      this.topic = builder.topic;
      this.keyDeserializer = builder.keyDeserializer;
      this.valueDeserializer = builder.valueDeserializer;
      this.rebalanceListener = builder.rebalanceListener;
    }

    @Override
    public LazyConsumer createConsumer() {
      return new LazyConsumer(consumerProperties, topic, keyDeserializer, valueDeserializer, rebalanceListener);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Configuration including defaults needed to consume spans from a Kafka topic. */
  public static final class Builder implements CollectorComponent.Builder {
    Collector.Builder delegate = Collector.builder(KafkaCollector.class);
    CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;
    ConsumerRebalanceListener rebalanceListener = null;
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

    /** Specifies a listener instance that should be invoked during consumer rebalancing */
    public Builder rebalanceListener(ConsumerRebalanceListener rebalanceListener) {
      this.rebalanceListener = rebalanceListener;
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
}
