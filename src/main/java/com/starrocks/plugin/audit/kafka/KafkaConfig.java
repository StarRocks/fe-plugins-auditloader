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

import java.util.Map;

/**
 * Configuration for Kafka producer
 */
public class KafkaConfig {
    // Connection
    private String bootstrapServers;
    private String topic;

    // Performance
    private int batchSize = 16384; // 16KB
    private int lingerMs = 10;
    private String compressionType = "snappy";
    private long bufferMemory = 33554432; // 32MB

    // Reliability
    private String acks = "1";
    private int retries = 3;
    private int maxInFlightRequests = 5;

    // Timeouts
    private int requestTimeout = 30000;
    private int deliveryTimeout = 120000;

    // Security (optional)
    private String saslMechanism;
    private String saslJaasConfig;
    private String securityProtocol;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;

    public KafkaConfig(Map<String, String> configMap) {
        loadFromMap(configMap);
    }

    private void loadFromMap(Map<String, String> config) {
        // Connection
        this.bootstrapServers = config.getOrDefault("kafka.bootstrap.servers", "localhost:9092");
        this.topic = config.getOrDefault("kafka.topic", "starrocks_audit_logs");

        // Performance
        if (config.containsKey("kafka.batch.size")) {
            this.batchSize = Integer.parseInt(config.get("kafka.batch.size"));
        }
        if (config.containsKey("kafka.linger.ms")) {
            this.lingerMs = Integer.parseInt(config.get("kafka.linger.ms"));
        }
        this.compressionType = config.getOrDefault("kafka.compression.type", "snappy");
        if (config.containsKey("kafka.buffer.memory")) {
            this.bufferMemory = Long.parseLong(config.get("kafka.buffer.memory"));
        }

        // Reliability
        this.acks = config.getOrDefault("kafka.acks", "1");
        if (config.containsKey("kafka.retries")) {
            this.retries = Integer.parseInt(config.get("kafka.retries"));
        }
        if (config.containsKey("kafka.max.in.flight.requests.per.connection")) {
            this.maxInFlightRequests = Integer.parseInt(config.get("kafka.max.in.flight.requests.per.connection"));
        }

        // Timeouts
        if (config.containsKey("kafka.request.timeout.ms")) {
            this.requestTimeout = Integer.parseInt(config.get("kafka.request.timeout.ms"));
        }
        if (config.containsKey("kafka.delivery.timeout.ms")) {
            this.deliveryTimeout = Integer.parseInt(config.get("kafka.delivery.timeout.ms"));
        }

        // Security
        this.saslMechanism = config.get("kafka.sasl.mechanism");
        this.saslJaasConfig = config.get("kafka.sasl.jaas.config");
        this.securityProtocol = config.get("kafka.security.protocol");
        this.sslTruststoreLocation = config.get("kafka.ssl.truststore.location");
        this.sslTruststorePassword = config.get("kafka.ssl.truststore.password");
    }

    // Getters
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public String getAcks() {
        return acks;
    }

    public int getRetries() {
        return retries;
    }

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", batchSize=" + batchSize +
                ", lingerMs=" + lingerMs +
                ", compressionType='" + compressionType + '\'' +
                ", bufferMemory=" + bufferMemory +
                ", acks='" + acks + '\'' +
                ", retries=" + retries +
                ", maxInFlightRequests=" + maxInFlightRequests +
                ", requestTimeout=" + requestTimeout +
                ", deliveryTimeout=" + deliveryTimeout +
                '}';
    }
}
