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

package com.starrocks.plugin.audit.routing;

import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.audit.output.OutputHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Router for distributing events to multiple output handlers
 */
public class OutputRouter {
    private static final Logger LOG = LogManager.getLogger(OutputRouter.class);

    private List<OutputHandler> handlers;
    private RoutingMode mode;

    /**
     * Routing modes
     */
    public enum RoutingMode {
        SINGLE,      // Use only one handler
        DUAL,        // Use all handlers in parallel
        FALLBACK     // Use primary, fallback to secondary on failure
    }

    public OutputRouter(RoutingMode mode) {
        this.mode = mode;
        this.handlers = new ArrayList<>();
    }

    /**
     * Add an output handler
     */
    public void addHandler(OutputHandler handler) {
        handlers.add(handler);
        LOG.info("Added output handler: {}", handler.getName());
    }

    /**
     * Route events to handlers based on mode
     */
    public void route(List<AuditEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        switch (mode) {
            case SINGLE:
                routeSingle(events);
                break;
            case DUAL:
                routeDual(events);
                break;
            case FALLBACK:
                routeFallback(events);
                break;
        }
    }

    /**
     * Route to first handler only
     */
    private void routeSingle(List<AuditEvent> events) {
        if (handlers.isEmpty()) {
            LOG.warn("No handlers configured");
            return;
        }

        OutputHandler handler = handlers.get(0);
        try {
            handler.send(events);
        } catch (Exception e) {
            LOG.error("Failed to send events via {}", handler.getName(), e);
        }
    }

    /**
     * Route to all handlers in parallel
     */
    private void routeDual(List<AuditEvent> events) {
        int successCount = 0;
        int failureCount = 0;

        for (OutputHandler handler : handlers) {
            try {
                handler.send(events);
                successCount++;
            } catch (Exception e) {
                failureCount++;
                LOG.error("Failed to send events via {}", handler.getName(), e);
                // Continue to next handler
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Dual routing completed: {} succeeded, {} failed",
                     successCount, failureCount);
        }
    }

    /**
     * Route to handlers with fallback
     * Try primary first, fallback to secondary on failure
     */
    private void routeFallback(List<AuditEvent> events) {
        for (int i = 0; i < handlers.size(); i++) {
            OutputHandler handler = handlers.get(i);
            try {
                handler.send(events);
                if (i > 0) {
                    LOG.info("Successfully sent via fallback handler: {}", handler.getName());
                }
                return; // Success, stop trying
            } catch (Exception e) {
                LOG.warn("Failed to send via {}, trying next handler",
                        handler.getName(), e);
            }
        }

        LOG.error("All handlers failed to send events");
    }

    /**
     * Close all handlers
     */
    public void close() {
        for (OutputHandler handler : handlers) {
            try {
                handler.close();
            } catch (Exception e) {
                LOG.error("Error closing handler: {}", handler.getName(), e);
            }
        }
    }

    /**
     * Check if any handler is healthy
     */
    public boolean isHealthy() {
        for (OutputHandler handler : handlers) {
            if (handler.isHealthy()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get all handlers
     */
    public List<OutputHandler> getHandlers() {
        return handlers;
    }

    /**
     * Get routing mode
     */
    public RoutingMode getMode() {
        return mode;
    }
}
