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

package com.starrocks.plugin.audit.output;

import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.audit.kafka.KafkaProducerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Kafka output handler
 * Sends audit logs to Apache Kafka
 */
public class KafkaOutputHandler implements OutputHandler {
    private static final Logger LOG = LogManager.getLogger(KafkaOutputHandler.class);

    private KafkaProducerManager producerManager;
    private String topic;
    private boolean asyncMode;

    @Override
    public void init(Map<String, String> config) throws Exception {
        this.topic = config.getOrDefault("kafka.topic", "starrocks_audit_logs");
        this.asyncMode = Boolean.parseBoolean(
            config.getOrDefault("kafka.async_mode", "true")
        );

        this.producerManager = new KafkaProducerManager(config);
        this.producerManager.init();

        LOG.info("KafkaOutputHandler initialized: topic={}, async={}", topic, asyncMode);
    }

    @Override
    public void send(List<AuditEvent> events) throws Exception {
        if (events == null || events.isEmpty()) {
            return;
        }

        for (AuditEvent event : events) {
            if (asyncMode) {
                producerManager.sendAsync(topic, event);
            } else {
                producerManager.sendSync(topic, event);
            }
        }

        // Flush if not async to ensure delivery
        if (!asyncMode) {
            producerManager.flush();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sent {} events to Kafka topic: {}", events.size(), topic);
        }
    }

    @Override
    public void close() throws Exception {
        if (producerManager != null) {
            producerManager.close();
        }
        LOG.info("KafkaOutputHandler closed");
    }

    @Override
    public String getName() {
        return "KafkaOutputHandler";
    }

    @Override
    public boolean isHealthy() {
        return producerManager != null && producerManager.isHealthy();
    }

    /**
     * Get producer manager for metrics access
     */
    public KafkaProducerManager getProducerManager() {
        return producerManager;
    }
}
