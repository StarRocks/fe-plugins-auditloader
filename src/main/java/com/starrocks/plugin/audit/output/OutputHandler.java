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

import java.util.List;
import java.util.Map;

/**
 * Interface for audit log output handlers.
 * Abstracts different output destinations (StarRocks, Kafka, etc.)
 */
public interface OutputHandler {

    /**
     * Initialize the handler with configuration
     * @param config Configuration map
     * @throws Exception if initialization fails
     */
    void init(Map<String, String> config) throws Exception;

    /**
     * Send a batch of events
     * @param events List of events to send
     * @throws Exception if sending fails
     */
    void send(List<AuditEvent> events) throws Exception;

    /**
     * Close resources and cleanup
     * @throws Exception if closing fails
     */
    void close() throws Exception;

    /**
     * Get handler name
     * @return Handler name
     */
    String getName();

    /**
     * Check if handler is healthy
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
}
