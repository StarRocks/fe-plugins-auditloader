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
import com.starrocks.plugin.audit.AuditLoaderPlugin;
import com.starrocks.plugin.audit.StarrocksStreamLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Stream Load output handler
 * Wraps existing StarRocks Stream Load functionality
 */
public class StreamLoadOutputHandler implements OutputHandler {
    private static final Logger LOG = LogManager.getLogger(StreamLoadOutputHandler.class);
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private StarrocksStreamLoader streamLoader;
    private StringBuilder auditBuffer;
    private boolean candidateMvsExists;
    private boolean hitMVsExists;
    private int maxStmtLength;
    private int qeSlowLogMs;

    @Override
    public void init(Map<String, String> config) throws Exception {
        // Create AuditLoaderConf from config
        AuditLoaderPlugin.AuditLoaderConf conf = new AuditLoaderPlugin.AuditLoaderConf();
        conf.init(config);

        this.streamLoader = new StarrocksStreamLoader(conf);
        this.auditBuffer = new StringBuilder();
        this.maxStmtLength = conf.maxStmtLength;
        this.qeSlowLogMs = conf.qeSlowLogMs;

        // Check if new fields exist
        this.candidateMvsExists = hasField(AuditEvent.class, "candidateMvs");
        this.hitMVsExists = hasField(AuditEvent.class, "hitMVs");

        LOG.info("StreamLoadOutputHandler initialized");
    }

    @Override
    public void send(List<AuditEvent> events) throws Exception {
        if (events == null || events.isEmpty()) {
            return;
        }

        // Assemble events into JSON array
        for (AuditEvent event : events) {
            assembleAudit(event);
        }

        // Send batch
        if (auditBuffer.length() > 0) {
            try {
                StarrocksStreamLoader.LoadResponse response = streamLoader.loadBatch(auditBuffer);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stream load response: {}", response);
                }
            } finally {
                // Clear buffer for next batch
                auditBuffer = new StringBuilder();
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("StreamLoadOutputHandler closed");
    }

    @Override
    public String getName() {
        return "StreamLoadOutputHandler";
    }

    @Override
    public boolean isHealthy() {
        return streamLoader != null;
    }

    /**
     * Assemble audit event to JSON format (same logic as original plugin)
     */
    private void assembleAudit(AuditEvent event) {
        String queryType = getQueryType(event);
        int isQuery = event.isQuery ? 1 : 0;

        String candidateMvsVal = candidateMvsExists ? getFieldValue(event, "candidateMvs", "") : "";
        String hitMVsVal = hitMVsExists ? getFieldValue(event, "hitMVs", "") : "";

        String content = "{\"queryId\":\"" + getQueryId(queryType, event) + "\"," +
                "\"timestamp\":\"" + longToTimeString(event.timestamp) + "\"," +
                "\"queryType\":\"" + queryType + "\"," +
                "\"clientIp\":\"" + escapeJson(event.clientIp) + "\"," +
                "\"user\":\"" + escapeJson(event.user) + "\"," +
                "\"authorizedUser\":\"" + escapeJson(event.authorizedUser) + "\"," +
                "\"resourceGroup\":\"" + escapeJson(event.resourceGroup) + "\"," +
                "\"catalog\":\"" + escapeJson(event.catalog) + "\"," +
                "\"db\":\"" + escapeJson(event.db) + "\"," +
                "\"state\":\"" + escapeJson(event.state) + "\"," +
                "\"errorCode\":\"" + escapeJson(event.errorCode) + "\"," +
                "\"queryTime\":" + event.queryTime + "," +
                "\"scanBytes\":" + event.scanBytes + "," +
                "\"scanRows\":" + event.scanRows + "," +
                "\"returnRows\":" + event.returnRows + "," +
                "\"cpuCostNs\":" + event.cpuCostNs + "," +
                "\"memCostBytes\":" + event.memCostBytes + "," +
                "\"stmtId\":" + event.stmtId + "," +
                "\"isQuery\":" + isQuery + "," +
                "\"feIp\":\"" + escapeJson(event.feIp) + "\"," +
                "\"stmt\":\"" + escapeJson(truncateByBytes(event.stmt)) + "\"," +
                "\"digest\":\"" + escapeJson(event.digest) + "\"," +
                "\"planCpuCosts\":" + event.planCpuCosts + "," +
                "\"planMemCosts\":" + event.planMemCosts + "," +
                "\"pendingTimeMs\":" + event.pendingTimeMs + "," +
                "\"candidateMVs\":\"" + escapeJson(candidateMvsVal) + "\"," +
                "\"hitMvs\":\"" + escapeJson(hitMVsVal) + "\"," +
                "\"warehouse\":\"" + escapeJson(event.warehouse) + "\"}";

        if (auditBuffer.length() > 0) {
            auditBuffer.append(",");
        }
        auditBuffer.append(content);
    }

    private String getQueryId(String prefix, AuditEvent event) {
        return (Objects.isNull(event.queryId) || event.queryId.isEmpty()) ?
                prefix + "-" + UUID.randomUUID() : event.queryId;
    }

    private String getQueryType(AuditEvent event) {
        try {
            switch (event.type) {
                case CONNECTION:
                    return "connection";
                case DISCONNECTION:
                    return "disconnection";
                default:
                    return (event.queryTime > qeSlowLogMs) ? "slow_query" : "query";
            }
        } catch (Exception e) {
            return (event.queryTime > qeSlowLogMs) ? "slow_query" : "query";
        }
    }

    private String truncateByBytes(String str) {
        if (str == null) {
            return "";
        }
        int maxLen = Math.min(maxStmtLength, str.getBytes().length);
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        // Simple truncation (original has more complex logic)
        return str.substring(0, Math.min(str.length(), maxStmtLength));
    }

    private String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return DATETIME_FORMAT.format(new Date());
        }
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }

    private String escapeJson(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    private boolean hasField(Class<?> clazz, String fieldName) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.getName().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    private String getFieldValue(AuditEvent event, String fieldName, String defaultValue) {
        try {
            Field field = AuditEvent.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(event);
            return value != null ? value.toString() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
