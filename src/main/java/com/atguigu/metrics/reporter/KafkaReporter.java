package com.atguigu.metrics.reporter;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.atguigu.metrics.reporter.KafkaReporterOptions.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>将Flink Metrics写入Kafka</p>
 *
 * @author : realdengziqi
 * @date : 2022-06-13 13:42
 **/
public class KafkaReporter extends AbstractReporter implements MetricReporter, CharacterFilter, Scheduled {
    private static final Logger log = LoggerFactory.getLogger(KafkaReporter.class);

    private static final char SCOPE_SEPARATOR = '_';
    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
        @Override
        public String filterCharacters(String input) {
            return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
        }
    };

    // the initial size roughly fits ~150 metrics with default scope settings

    private KafkaProducer<String, String> kafkaProducer;
    private String servers;
    private String topic;
    private String keyBy;
    private String cluster;

    protected final Map<Gauge<?>, Map<String, String>> gauges = new HashMap<>();
    protected final Map<Counter, Map<String, String>> counters = new HashMap<>();
    protected final Map<Histogram, Map<String, String>> histograms = new HashMap<>();
    protected final Map<Meter, Map<String, String>> meters = new HashMap<>();

    private Long notifyMetricsCalledNum = 0L;

    private ObjectMapper mapper = new ObjectMapper();



    @Override
    public void open(MetricConfig config) {
        log.info("kafkaReporter初始化");
        cluster = config.getString(CLUSTER.key(), CLUSTER.defaultValue());
        servers = config.getString(SERVERS.key(), SERVERS.defaultValue());
        topic = config.getString(TOPIC.key(), TOPIC.defaultValue());
        keyBy = config.getString(KEY_BY.key(), KEY_BY.defaultValue());
        if (servers == null) {
            log.warn("Cannot find config {}", SERVERS.key());
        }


        Properties properties = new Properties();
        properties.put(SERVERS.key(), servers);
        for (Object keyObj : config.keySet()) {
            String key = keyObj.toString();
            if (key.startsWith("prop.")) {
                properties.put(key.substring(5), config.getString(key, ""));
            }
        }

        try {
            Thread.currentThread().setContextClassLoader(null);
            kafkaProducer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        } catch (Exception e) {
            log.warn("KafkaReporter init error.", e);
        }
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    /**
     * 注册指标,
     * @param metric
     * @param metricName
     * @param group
     */
    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> allVariables = new HashMap<>();
        allVariables.put("name", getLogicalScope(group) + SCOPE_SEPARATOR + CHARACTER_FILTER.filterCharacters(metricName));
        for (Map.Entry<String, String> entry : group.getAllVariables().entrySet()) {
            String key = entry.getKey();
            allVariables.put(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)), entry.getValue());
        }
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.put((Counter) metric, allVariables);
            } else if (metric instanceof Gauge) {
                this.gauges.put((Gauge) metric, allVariables);
            } else if (metric instanceof Histogram) {
                this.histograms.put((Histogram) metric, allVariables);
            } else if (metric instanceof Meter) {
                this.meters.put((Meter) metric, allVariables);
            } else {
                log.warn("Cannot add unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }

    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.remove(metric);
            } else if (metric instanceof Gauge) {
                this.gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                this.histograms.remove(metric);
            } else if (metric instanceof Meter) {
                this.meters.remove(metric);
            } else {
                log.warn("Cannot remove unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }
    }

    private static String getLogicalScope(MetricGroup group) {
        return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    }

    @Override
    public void report() {
        try {
            tryReport();
        } catch (Exception ignored) {
            log.warn("KafkaReporter report error: {}", ignored.getMessage());
        }
    }

    private void tryReport() throws JsonProcessingException {
        if (kafkaProducer == null) {
            return;
        }
        long timeStamp = System.currentTimeMillis();
        for (Map.Entry<Counter, Map<String, String>> metric : counters.entrySet()) {
            Map<String, String> metricValue = new HashMap<>(metric.getValue());

            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("type", "counter");
            json.put("name", metricValue.remove("name"));
            json.put("host", metricValue.remove("host"));
            json.put("time_stamp", timeStamp);
            json.put("metric", metricValue);
            json.put("value", Collections.singletonMap("count", metric.getKey().getCount()));
            kafkaProducer.send(new ProducerRecord<>(topic, getKey(json), mapper.writeValueAsString(json)));
        }

        for (Map.Entry<Gauge<?>, Map<String, String>> metric : gauges.entrySet()) {
            Map<String, String> metricValue = new HashMap<>(metric.getValue());

            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("type", "gauge");
            json.put("name", metricValue.remove("name"));
            json.put("host", metricValue.remove("host"));
            json.put("time_stamp", timeStamp);
            json.put("metric", metricValue);
            json.put("value", Collections.singletonMap("value", metric.getKey().getValue()));
            kafkaProducer.send(new ProducerRecord<>(topic, getKey(json), mapper.writeValueAsString(json)));
        }

        for (Map.Entry<Meter, Map<String, String>> metric : meters.entrySet()) {
            Map<String, String> metricValue = new HashMap<>(metric.getValue());
            Meter meter = metric.getKey();

            Map<String, Object> value = new HashMap<>();
            value.put("count", meter.getCount());
            value.put("rate", meter.getRate());

            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("type", "meter");
            json.put("name", metricValue.remove("name"));
            json.put("host", metricValue.remove("host"));
            json.put("time_stamp", timeStamp);
            json.put("metric", metricValue);
            json.put("value", value);
            kafkaProducer.send(new ProducerRecord<>(topic, getKey(json), mapper.writeValueAsString(json)));
        }


        for (Map.Entry<Histogram, Map<String, String>> metric : histograms.entrySet()) {
            Map<String, String> metricValue = new HashMap<>(metric.getValue());
            HistogramStatistics stats = metric.getKey().getStatistics();

            Map<String, Object> value = new HashMap<>();
            value.put("count", stats.size());
            value.put("min", stats.getMin());
            value.put("max", stats.getMax());
            value.put("mean", stats.getMean());
            value.put("stddev", stats.getStdDev());
            value.put("p50", stats.getQuantile(0.50));
            value.put("p75", stats.getQuantile(0.75));
            value.put("p95", stats.getQuantile(0.95));
            value.put("p98", stats.getQuantile(0.98));
            value.put("p99", stats.getQuantile(0.99));
            value.put("p999", stats.getQuantile(0.999));

            Map<String, Object> json = new HashMap<>();
            json.put("cluster", cluster);
            json.put("type", "histogram");
            json.put("name", metricValue.remove("name"));
            json.put("host", metricValue.remove("host"));
            json.put("time_stamp", timeStamp);
            json.put("metric", metricValue);
            json.put("value", value);
            kafkaProducer.send(new ProducerRecord<>(topic, getKey(json), mapper.writeValueAsString(json)));
        }
    }

    private String getKey(Map<String, Object> json) throws JsonProcessingException {
        if (keyBy.isEmpty()) {
            return null;
        }
        //noinspection unchecked
        Map<String, String> metricValue = (Map<String, String>) json.get("metric");
        if(keyBy.equals("metric")) {
            return mapper.writeValueAsString(metricValue);
        }
        Object keyValue = Optional.ofNullable(json.get(keyBy)).orElse(metricValue.get(keyBy));
        return keyValue == null ? null : keyValue.toString();
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
