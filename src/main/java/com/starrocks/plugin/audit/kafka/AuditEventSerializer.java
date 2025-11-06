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

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Serializer for AuditEvent to JSON string
 */
public class AuditEventSerializer {
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // Check if new fields exist (for backward compatibility)
    private final boolean candidateMvsExists;
    private final boolean hitMvsExists;

    public AuditEventSerializer() {
        this.candidateMvsExists = hasField(AuditEvent.class, "candidateMvs");
        this.hitMvsExists = hasField(AuditEvent.class, "hitMVs");
    }

    /**
     * Serialize AuditEvent to JSON string
     */
    public String serialize(AuditEvent event) {
        String queryType = determineQueryType(event);
        String queryId = getQueryId(queryType, event);
        int isQuery = event.isQuery ? 1 : 0;

        String candidateMvsVal = candidateMvsExists ? getFieldValue(event, "candidateMvs", "") : "";
        String hitMvsVal = hitMvsExists ? getFieldValue(event, "hitMVs", "") : "";

        return "{\"queryId\":\"" + escapeJson(queryId) + "\"," +
                "\"timestamp\":\"" + formatTimestamp(event.timestamp) + "\"," +
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
                "\"stmt\":\"" + escapeJson(event.stmt) + "\"," +
                "\"digest\":\"" + escapeJson(event.digest) + "\"," +
                "\"planCpuCosts\":" + event.planCpuCosts + "," +
                "\"planMemCosts\":" + event.planMemCosts + "," +
                "\"pendingTimeMs\":" + event.pendingTimeMs + "," +
                "\"candidateMVs\":\"" + escapeJson(candidateMvsVal) + "\"," +
                "\"hitMvs\":\"" + escapeJson(hitMvsVal) + "\"," +
                "\"warehouse\":\"" + escapeJson(event.warehouse) + "\"}";
    }

    private String getQueryId(String prefix, AuditEvent event) {
        if (event.queryId == null || event.queryId.isEmpty()) {
            return prefix + "-" + UUID.randomUUID();
        }
        return event.queryId;
    }

    private String determineQueryType(AuditEvent event) {
        try {
            switch (event.type) {
                case CONNECTION:
                    return "connection";
                case DISCONNECTION:
                    return "disconnection";
                default:
                    return "query";
            }
        } catch (Exception e) {
            return "query";
        }
    }

    private String formatTimestamp(long timestamp) {
        if (timestamp <= 0L) {
            return DATETIME_FORMAT.format(new Date());
        }
        return DATETIME_FORMAT.format(new Date(timestamp));
    }

    /**
     * Escape special characters for JSON
     */
    private String escapeJson(String str) {
        if (str == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            switch (ch) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (ch < ' ') {
                        String ss = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int k = 0; k < 4 - ss.length(); k++) {
                            sb.append('0');
                        }
                        sb.append(ss.toUpperCase());
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * Check if a field exists in the class
     */
    private boolean hasField(Class<?> clazz, String fieldName) {
        try {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (field.getName().equals(fieldName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get field value using reflection
     */
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
