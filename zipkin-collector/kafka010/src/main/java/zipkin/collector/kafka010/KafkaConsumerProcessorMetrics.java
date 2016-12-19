package zipkin.collector.kafka010;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages metrics for a single processor instance attached to a particular KafkaConsumer.
 */
public class KafkaConsumerProcessorMetrics {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerProcessorMetrics.class);

    private static final Field METRICS_FIELD;

    // private non-final double
    private static final Field MIN_INITIAL_VALUE_FIELD;

    static {
        Field metricsField;
        try {
            metricsField = KafkaConsumer.class.getDeclaredField("metrics");
            metricsField.setAccessible(true);
        } catch (Exception e) {
            logger.error("Unable to make KafkaConsumer.metrics field accessible!", e);
            metricsField = null;
        }
        METRICS_FIELD = metricsField;

        Field initialValueField;
        try {
            initialValueField = Min.class.getDeclaredField("initialValue");
            initialValueField.setAccessible(true);
        } catch (Exception e) {
            logger.error("Unable to locate the {}.initialValue (double) field using reflection!", Min.class, e);
            initialValueField = null;
        }
        MIN_INITIAL_VALUE_FIELD = initialValueField;
    }

    private static final char METRIC_NAME_SEPARATOR = '.';

    private static final String SENSOR_PREFIX = "consumer-runner-%s";
    private static final String CONSUMER_METRICS_GROUP = "consumer-metrics";

    private final ConcurrentMap<String, Sensor> throwableToSensorMap = new ConcurrentHashMap<>();
    private final Sensor totalErrorSensor;
    private final Sensor processTimeSensor;
    private final String metricPrefix;
    private final Metrics metrics;
    private final boolean isBatch;

    KafkaConsumerProcessorMetrics(Metrics metrics, String metricPrefix) {
        this(metrics, metricPrefix, false);
    }

    KafkaConsumerProcessorMetrics(Metrics metrics, String metricPrefix, boolean isBatch) {
        this.metrics = metrics;
        this.metricPrefix = metricPrefix;
        this.totalErrorSensor = initTotalErrorSensor();
        this.processTimeSensor = initProcessTimeSensor();
        this.isBatch = isBatch;
    }

    void recordTotalError() {
        this.totalErrorSensor.record();
    }

    void recordThrowable(Throwable throwable) {
        getOrCreateThrowableSensor(throwable).record();
    }

    void recordProcessTime(long elapsedMillis) {
        this.processTimeSensor.record(elapsedMillis);
    }

    private Sensor initTotalErrorSensor() {
        Sensor sensor = this.metrics.sensor(String.format(SENSOR_PREFIX, "total-errors"));
        final String name = buildMetricName(metricPrefix, "total invalid msg rate");
        sensor.add(this.metrics.metricName(name, CONSUMER_METRICS_GROUP,
                "Total number of messages that failed processing"), new Count());
        return sensor;
    }

    private Sensor initProcessTimeSensor() {
        String descSuffix = isBatch ? "per batch" : "per msg";
        Sensor sensor = this.metrics.sensor(String.format(SENSOR_PREFIX, "process-time"));
        final String avgName = buildMetricName(metricPrefix, "avg process time " + descSuffix);
        sensor.add(this.metrics.metricName(avgName, CONSUMER_METRICS_GROUP,
                "Average process time"), new Avg());
        final String minName = buildMetricName(metricPrefix, "min process time " + descSuffix);
        sensor.add(this.metrics.metricName(minName, CONSUMER_METRICS_GROUP,
                "Minimum process time"), minWithDefaultZero());
        final String maxName = buildMetricName(metricPrefix, "max process time " + descSuffix);
        sensor.add(this.metrics.metricName(maxName, CONSUMER_METRICS_GROUP,
                "Maximum process time"), new Max());
        return sensor;
    }

    private Sensor getOrCreateThrowableSensor(Throwable throwable) {
        final String simpleName = throwable.getClass().getSimpleName();
        if (!throwableToSensorMap.containsKey(simpleName)) {
            final String name = buildMetricName(metricPrefix, "errors", simpleName);
            Sensor throwableSensor = this.metrics.sensor(String.format(SENSOR_PREFIX, simpleName));
            throwableSensor.add(this.metrics.metricName(name, CONSUMER_METRICS_GROUP,
                    "Number of Throwables"), new Count());
            throwableToSensorMap.putIfAbsent(simpleName, throwableSensor);
        }
        return throwableToSensorMap.get(simpleName);
    }

    private static String buildMetricName(Object ... pathComponents) {
        StringBuilder sb = new StringBuilder(192);
        for (Object pathSegment : pathComponents) {
            if (sb.length() > 0) {
                sb.append(METRIC_NAME_SEPARATOR);
            }
            sb.append(pathSegment);
        }
        return sb.toString();
    }

    private static Min minWithDefaultZero() {
        Min min = new Min();
        if (MIN_INITIAL_VALUE_FIELD != null) {
            try {
                // force the initialValue field to 0.00
                MIN_INITIAL_VALUE_FIELD.setDouble(min, 0d);
            } catch (Exception e) {
                logger.error("The default initialValue could not be set to 0.00", e);
            }
        }
        return min;
    }

    /**
     * Extracts the <tt>metrics</tt> field from the provided KafkaConsumer and wraps it in our own
     * KafkaConsumerProcessorMetrics class, for use in this and associated Zipkin Kafka Collector classes
     */
    static KafkaConsumerProcessorMetrics forKafkaConsumer(KafkaConsumer<?, ?> kafkaConsumer,
                                                                 String metricPrefix, boolean isBatch) {
        try {
            Metrics metrics = (Metrics) METRICS_FIELD.get(kafkaConsumer);
            return new KafkaConsumerProcessorMetrics(metrics, metricPrefix, isBatch);
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Unable to extract value from 'metrics' field in KafkaConsumer: %s",
                    kafkaConsumer), e);
        }
    }
}
