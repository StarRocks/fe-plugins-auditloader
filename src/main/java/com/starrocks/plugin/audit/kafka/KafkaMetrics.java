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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for Kafka producer
 */
public class KafkaMetrics {
    // Counters
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong totalMessages = new AtomicLong(0);

    // Latency statistics
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final AtomicLong maxLatency = new AtomicLong(0);
    private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);

    // Batch statistics
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalBytes = new AtomicLong(0);

    public void recordSuccess() {
        successCount.incrementAndGet();
        totalMessages.incrementAndGet();
    }

    public void recordFailure() {
        failureCount.incrementAndGet();
        totalMessages.incrementAndGet();
    }

    public void recordLatency(long latencyMs) {
        totalLatency.addAndGet(latencyMs);

        // Update max
        long current = maxLatency.get();
        while (latencyMs > current && !maxLatency.compareAndSet(current, latencyMs)) {
            current = maxLatency.get();
        }

        // Update min
        current = minLatency.get();
        while (latencyMs < current && !minLatency.compareAndSet(current, latencyMs)) {
            current = minLatency.get();
        }
    }

    public void recordBatch(int messageCount, int bytes) {
        totalBatches.incrementAndGet();
        totalBytes.addAndGet(bytes);
    }

    public MetricsSnapshot getSnapshot() {
        long success = successCount.get();
        long failure = failureCount.get();
        long total = totalMessages.get();

        double successRate = total > 0 ? (double) success / total * 100 : 0;
        double avgLatency = success > 0 ? (double) totalLatency.get() / success : 0;

        long min = minLatency.get();
        if (min == Long.MAX_VALUE) {
            min = 0;
        }

        return new MetricsSnapshot(
            success, failure, total, successRate,
            avgLatency, min, maxLatency.get(),
            totalBatches.get(), totalBytes.get()
        );
    }

    public void reset() {
        successCount.set(0);
        failureCount.set(0);
        totalMessages.set(0);
        totalLatency.set(0);
        maxLatency.set(0);
        minLatency.set(Long.MAX_VALUE);
        totalBatches.set(0);
        totalBytes.set(0);
    }

    /**
     * Metrics snapshot
     */
    public static class MetricsSnapshot {
        public final long successCount;
        public final long failureCount;
        public final long totalCount;
        public final double successRate;
        public final double avgLatencyMs;
        public final long minLatencyMs;
        public final long maxLatencyMs;
        public final long totalBatches;
        public final long totalBytes;

        public MetricsSnapshot(long success, long failure, long total,
                             double successRate, double avgLatency,
                             long minLatency, long maxLatency,
                             long batches, long bytes) {
            this.successCount = success;
            this.failureCount = failure;
            this.totalCount = total;
            this.successRate = successRate;
            this.avgLatencyMs = avgLatency;
            this.minLatencyMs = minLatency;
            this.maxLatencyMs = maxLatency;
            this.totalBatches = batches;
            this.totalBytes = bytes;
        }

        @Override
        public String toString() {
            return String.format(
                "Metrics[success=%d, failure=%d, rate=%.2f%%, " +
                "latency(avg/min/max)=%.2f/%d/%d ms, " +
                "batches=%d, bytes=%d]",
                successCount, failureCount, successRate,
                avgLatencyMs, minLatencyMs, maxLatencyMs,
                totalBatches, totalBytes
            );
        }
    }
}
