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

package com.starrocks.plugin.audit;

import com.starrocks.plugin.*;
import com.starrocks.plugin.audit.output.KafkaOutputHandler;
import com.starrocks.plugin.audit.output.OutputHandler;
import com.starrocks.plugin.audit.output.StreamLoadOutputHandler;
import com.starrocks.plugin.audit.routing.OutputRouter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load audit log to specified starrocks table at specified interval
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // CSV format constants for Stream Load
    public static final char COLUMN_SEPARATOR = '\t';
    public static final char ROW_DELIMITER = '\n';

    private long lastLoadTime = 0;
    private BlockingQueue<AuditEvent> auditEventQueue;
    private List<AuditEvent> eventBatch;
    private OutputRouter outputRouter;
    private Thread loadThread;

    private AuditLoaderConf conf;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    /**
     * 是否包含新字段 candidateMvs，如果旧版本没有该字段则值为空
     */
    private boolean candidateMvsExists;
    /**
     * 是否包含新字段 hitMVsExists，如果旧版本没有该字段则值为空
     */
    private boolean hitMVsExists;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            this.lastLoadTime = System.currentTimeMillis();

            loadConfig(ctx, info.getProperties());
            this.auditEventQueue = new LinkedBlockingQueue<>(conf.maxQueueSize);
            this.eventBatch = new ArrayList<>();

            // Initialize output router
            initializeOutputRouter();

            this.loadThread = new Thread(new LoadWorker(), "audit loader thread");
            this.loadThread.setDaemon(true);
            this.loadThread.start();

            candidateMvsExists = hasField(AuditEvent.class, "candidateMvs");
            hitMVsExists = hasField(AuditEvent.class, "hitMVs");

            isInit = true;
        }
    }

    /**
     * Initialize output router based on configuration
     */
    private void initializeOutputRouter() throws PluginException {
        String outputMode = conf.properties.getOrDefault("output_mode", "streamload");

        OutputRouter.RoutingMode routingMode;
        switch (outputMode.toLowerCase()) {
            case "kafka":
                routingMode = OutputRouter.RoutingMode.SINGLE;
                break;
            case "dual":
                routingMode = OutputRouter.RoutingMode.DUAL;
                break;
            case "fallback":
                routingMode = OutputRouter.RoutingMode.FALLBACK;
                break;
            case "streamload":
            default:
                routingMode = OutputRouter.RoutingMode.SINGLE;
                break;
        }

        this.outputRouter = new OutputRouter(routingMode);

        // Initialize handlers based on mode
        try {
            if ("kafka".equalsIgnoreCase(outputMode)) {
                // Kafka only
                boolean kafkaEnabled = Boolean.parseBoolean(
                    conf.properties.getOrDefault("kafka.enabled", "false"));
                if (kafkaEnabled) {
                    KafkaOutputHandler kafkaHandler = new KafkaOutputHandler();
                    kafkaHandler.init(conf.properties);
                    outputRouter.addHandler(kafkaHandler);
                    LOG.info("Initialized Kafka output handler");
                } else {
                    throw new PluginException("Kafka output mode selected but kafka.enabled=false");
                }
            } else if ("dual".equalsIgnoreCase(outputMode)) {
                // Both Stream Load and Kafka
                boolean kafkaEnabled = Boolean.parseBoolean(
                    conf.properties.getOrDefault("kafka.enabled", "false"));

                // Add Stream Load handler
                StreamLoadOutputHandler streamHandler = new StreamLoadOutputHandler();
                streamHandler.init(conf.properties);
                outputRouter.addHandler(streamHandler);
                LOG.info("Initialized Stream Load output handler");

                // Add Kafka handler if enabled
                if (kafkaEnabled) {
                    KafkaOutputHandler kafkaHandler = new KafkaOutputHandler();
                    kafkaHandler.init(conf.properties);
                    outputRouter.addHandler(kafkaHandler);
                    LOG.info("Initialized Kafka output handler");
                }
            } else if ("fallback".equals(outputMode.toLowerCase())) {
                // Kafka primary, Stream Load fallback
                String primaryOutput = conf.properties.getOrDefault("primary_output", "kafka");
                String secondaryOutput = conf.properties.getOrDefault("secondary_output", "streamload");

                // Add primary handler
                if ("kafka".equals(primaryOutput)) {
                    boolean kafkaEnabled = Boolean.parseBoolean(
                        conf.properties.getOrDefault("kafka.enabled", "false"));
                    if (kafkaEnabled) {
                        KafkaOutputHandler kafkaHandler = new KafkaOutputHandler();
                        kafkaHandler.init(conf.properties);
                        outputRouter.addHandler(kafkaHandler);
                        LOG.info("Initialized Kafka as primary output handler");
                    }
                }

                // Add secondary handler
                if ("streamload".equals(secondaryOutput)) {
                    StreamLoadOutputHandler streamHandler = new StreamLoadOutputHandler();
                    streamHandler.init(conf.properties);
                    outputRouter.addHandler(streamHandler);
                    LOG.info("Initialized Stream Load as fallback output handler");
                }
            } else {
                // Default: Stream Load only
                StreamLoadOutputHandler streamHandler = new StreamLoadOutputHandler();
                streamHandler.init(conf.properties);
                outputRouter.addHandler(streamHandler);
                LOG.info("Initialized Stream Load output handler (default)");
            }
        } catch (Exception e) {
            throw new PluginException("Failed to initialize output handlers: " + e.getMessage(), e);
        }

        LOG.info("Output router initialized with mode: {}", outputMode);
    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath());
        if (!Files.exists(pluginPath)) {
            throw new PluginException("plugin path does not exist: " + pluginPath);
        }

        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile)) {
            throw new PluginException("plugin conf file does not exist: " + confFile);
        }

        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile)) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }

        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        final Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));
        conf = new AuditLoaderConf();
        conf.init(properties);
        conf.feIdentity = ctx.getFeIdentity();
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                LOG.info("waiting for the audit loader thread to complete execution");
                loadThread.join(conf.uninstallTimeout);
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
        }
        if (outputRouter != null) {
            outputRouter.close();
        }
        LOG.info("AuditLoader plugin is closed");
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY ||
                type == AuditEvent.EventType.CONNECTION;
    }

    public void exec(AuditEvent event) {
        try {
            auditEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            LOG.debug("encounter exception when putting current audit batch, discard current audit event", e);
        }
    }

    private void assembleAudit(AuditEvent event) {
        // Add event to batch for output router processing
        eventBatch.add(event);
    }

    private String getQueryId(String prefix, AuditEvent event) {
        return (Objects.isNull(event.queryId) || event.queryId.isEmpty()) ? prefix + "-" + UUID.randomUUID() : event.queryId;
    }

    private String getQueryType(AuditEvent event) {
        try {
            switch (event.type) {
                case CONNECTION:
                    return "connection";
                case DISCONNECTION:
                    return "disconnection";
                default:
                    return (event.queryTime > conf.qeSlowLogMs) ? "slow_query" : "query";
            }
        } catch (Exception e) {
            return (event.queryTime > conf.qeSlowLogMs) ? "slow_query" : "query";
        }
    }

    private String truncateByBytes(String str) {
        int maxLen = Math.min(conf.maxStmtLength, str.getBytes().length);
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        Charset utf8Charset = StandardCharsets.UTF_8;
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position());
    }

    private void loadIfNecessary() {
        if (eventBatch.size() < conf.maxBatchSize && System.currentTimeMillis() - lastLoadTime < conf.maxBatchIntervalSec * 1000) {
            return;
        }
        if (eventBatch.isEmpty()) {
            return;
        }

        lastLoadTime = System.currentTimeMillis();
        // begin to load
        try {
            outputRouter.route(new ArrayList<>(eventBatch));
            LOG.debug("audit loader batch sent successfully");
        } catch (Exception e) {
            LOG.error("encounter exception when routing current audit batch, discard current batch", e);
        } finally {
            // clear the batch for next round
            eventBatch.clear();
        }
    }

    /**
     * 类中是否包含指定字段
     *
     * @param clazz
     * @param fieldName
     * @return
     */
    private boolean hasField(Class<?> clazz, String fieldName) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.getName().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public static class AuditLoaderConf {
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";
        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";
        public static final String PROP_USER = "user";
        public static final String PROP_PASSWORD = "password";
        public static final String PROP_DATABASE = "database";
        public static final String PROP_TABLE = "table";
        // the max stmt length to be loaded in audit table.
        public static final String MAX_STMT_LENGTH = "max_stmt_length";
        public static final String QE_SLOW_LOG_MS = "qe_slow_log_ms";
        public static final String MAX_QUEUE_SIZE = "max_queue_size";
        public static final String STREAM_LOAD_FILTER = "filter";
        public static final String PROP_SECRET_KEY = "secret_key";
        public static final String PROP_UNINSTALL_TIMEOUT = "uninstall_timeout";

        public long maxBatchSize = 50 * 1024 * 1024;
        public long maxBatchIntervalSec = 60;
        public String frontendHostPort = "127.0.0.1:8030";
        public String user = "root";
        public String password = "";
        public String database = "starrocks_audit_db__";
        public String table = "starrocks_audit_tbl__";
        // the identity of FE which run this plugin
        public String feIdentity = "";
        public int maxStmtLength = 1048576;
        public int qeSlowLogMs = 5000;
        public int maxQueueSize = 1000;
        public String streamLoadFilter = "";
        public String secretKey = "";
        public long uninstallTimeout = 5;
        public Map<String, String> properties;

        public void init(Map<String, String> properties) throws PluginException {
            this.properties = properties;
            try {
                if (properties.containsKey(PROP_MAX_BATCH_SIZE)) {
                    maxBatchSize = Long.parseLong(properties.get(PROP_MAX_BATCH_SIZE));
                }
                if (properties.containsKey(PROP_MAX_BATCH_INTERVAL_SEC)) {
                    maxBatchIntervalSec = Long.parseLong(properties.get(PROP_MAX_BATCH_INTERVAL_SEC));
                }
                if (properties.containsKey(QE_SLOW_LOG_MS)) {
                    qeSlowLogMs = Integer.parseInt(properties.get(QE_SLOW_LOG_MS));
                }
                if (properties.containsKey(PROP_FRONTEND_HOST_PORT)) {
                    frontendHostPort = properties.get(PROP_FRONTEND_HOST_PORT);
                }
                if (properties.containsKey(PROP_USER)) {
                    user = properties.get(PROP_USER);
                }
                if (properties.containsKey(PROP_PASSWORD)) {
                    password = properties.get(PROP_PASSWORD);
                }
                if (properties.containsKey(PROP_SECRET_KEY)) {
                    secretKey = properties.get(PROP_SECRET_KEY);
                    if (secretKey.getBytes(StandardCharsets.UTF_8).length > 16) {
                        throw new PluginException("the maximum length of the key cannot exceed 16 bytes");
                    }
                }
                if (properties.containsKey(PROP_DATABASE)) {
                    database = properties.get(PROP_DATABASE);
                }
                if (properties.containsKey(PROP_TABLE)) {
                    table = properties.get(PROP_TABLE);
                }
                if (properties.containsKey(MAX_STMT_LENGTH)) {
                    maxStmtLength = Integer.parseInt(properties.get(MAX_STMT_LENGTH));
                }
                if (properties.containsKey(MAX_QUEUE_SIZE)) {
                    maxQueueSize = Integer.parseInt(properties.get(MAX_QUEUE_SIZE));
                }
                if (properties.containsKey(STREAM_LOAD_FILTER)) {
                    streamLoadFilter = properties.get(STREAM_LOAD_FILTER);
                }
                if (properties.containsKey(PROP_UNINSTALL_TIMEOUT)) {
                    uninstallTimeout = Long.parseLong(properties.get(PROP_UNINSTALL_TIMEOUT));
                }
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        public LoadWorker() {
        }

        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                    }
                    loadIfNecessary();
                } catch (InterruptedException ie) {
                    LOG.debug("encounter exception when loading current audit batch", ie);
                } catch (Exception e) {
                    LOG.error("run audit logger error:", e);
                }
            }
            LOG.info("audit loader thread run ends");
        }
    }

    public static synchronized String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return DATETIME_FORMAT.format(new Date());
        }
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }

}
