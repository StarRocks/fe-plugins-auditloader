// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.plugin.audit.kafka;

import com.starrocks.plugin.AuditEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager for Kafka producer lifecycle and message sending
 */
public class KafkaProducerManager {
    private static final Logger LOG = LogManager.getLogger(KafkaProducerManager.class);

    private KafkaProducer<String, String> producer;
    private AuditEventSerializer serializer;
    private KafkaConfig config;
    private KafkaMetrics metrics;

    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);

    public KafkaProducerManager(Map<String, String> configMap) {
        this.config = new KafkaConfig(configMap);
        this.serializer = new AuditEventSerializer();
        this.metrics = new KafkaMetrics();
    }

    /**
     * Initialize Kafka producer
     */
    public void init() {
        Properties props = new Properties();

        // Required settings
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Performance optimization
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getBufferMemory());

        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, config.getMaxInFlightRequests());

        // Timeout settings
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.getRequestTimeout());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, config.getDeliveryTimeout());

        // Security settings (if configured)
        if (config.getSaslMechanism() != null) {
            props.put("sasl.mechanism", config.getSaslMechanism());
        }
        if (config.getSaslJaasConfig() != null) {
            props.put("sasl.jaas.config", config.getSaslJaasConfig());
        }
        if (config.getSecurityProtocol() != null) {
            props.put("security.protocol", config.getSecurityProtocol());
        }
        if (config.getSslTruststoreLocation() != null) {
            props.put("ssl.truststore.location", config.getSslTruststoreLocation());
        }
        if (config.getSslTruststorePassword() != null) {
            props.put("ssl.truststore.password", config.getSslTruststorePassword());
        }

        this.producer = new KafkaProducer<>(props);

        LOG.info("Kafka Producer initialized with config: {}", config);
    }

    /**
     * Send event asynchronously (recommended for performance)
     */
    public void sendAsync(String topic, AuditEvent event) {
        try {
            String key = event.queryId; // Partitioning key
            String value = serializer.serialize(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            long startTime = System.currentTimeMillis();

            producer.send(record, (metadata, exception) -> {
                long latency = System.currentTimeMillis() - startTime;
                metrics.recordLatency(latency);

                if (exception != null) {
                    failureCount.incrementAndGet();
                    metrics.recordFailure();
                    LOG.error("Failed to send audit event: queryId={}", event.queryId, exception);
                } else {
                    successCount.incrementAndGet();
                    metrics.recordSuccess();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sent event to partition {} offset {}",
                                metadata.partition(), metadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            failureCount.incrementAndGet();
            metrics.recordFailure();
            LOG.error("Error sending audit event", e);
        }
    }

    /**
     * Send event synchronously (for reliability priority)
     */
    public RecordMetadata sendSync(String topic, AuditEvent event) throws Exception {
        String key = event.queryId;
        String value = serializer.serialize(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        long startTime = System.currentTimeMillis();

        try {
            RecordMetadata metadata = producer.send(record).get(
                    config.getRequestTimeout(), TimeUnit.MILLISECONDS
            );

            long latency = System.currentTimeMillis() - startTime;
            metrics.recordLatency(latency);
            successCount.incrementAndGet();
            metrics.recordSuccess();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Sync sent event to partition {} offset {}",
                        metadata.partition(), metadata.offset());
            }

            return metadata;

        } catch (Exception e) {
            failureCount.incrementAndGet();
            metrics.recordFailure();
            throw e;
        }
    }

    /**
     * Flush pending messages
     */
    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }

    /**
     * Close producer and cleanup
     */
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofMinutes(10));
        }

        LOG.info("Kafka Producer closed. Success: {}, Failures: {}",
                successCount.get(), failureCount.get());
    }

    /**
     * Check if producer is healthy
     */
    public boolean isHealthy() {
        if (producer == null) {
            return false;
        }

        // Check failure rate (unhealthy if > 10%)
        long total = successCount.get() + failureCount.get();
        if (total > 100) {
            double failureRate = (double) failureCount.get() / total;
            return failureRate < 0.1;
        }

        return true;
    }

    /**
     * Get metrics
     */
    public KafkaMetrics getMetrics() {
        return metrics;
    }

    /**
     * Get config
     */
    public KafkaConfig getConfig() {
        return config;
    }
}
