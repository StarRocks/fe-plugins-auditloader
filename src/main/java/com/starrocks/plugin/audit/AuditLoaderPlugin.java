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
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.SqlDigestBuilder;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.starrocks.plugin.audit.model.QueryEventData;
import java.util.concurrent.ExecutionException;

/*
 * This plugin will load audit log to specified starrocks table at specified interval
 */
public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private StringBuilder auditBuffer = new StringBuilder();
    private long lastLoadTime = 0;
    private BlockingQueue<AuditEvent> auditEventQueue;
    private StarrocksStreamLoader streamLoader;
    private Thread loadThread;

    private AuditLoaderConf conf;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    public static boolean kafkaEnable = false;

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
            this.streamLoader = new StarrocksStreamLoader(conf);
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "audit loader thread");
            this.loadThread.start();

            candidateMvsExists = hasField(AuditEvent.class, "candidateMvs");
            hitMVsExists = hasField(AuditEvent.class, "hitMVs");

            isInit = true;
        }
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
                loadThread.join();
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
        }
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
        String queryType = getQueryType(event);
        int isQuery = event.isQuery ? 1 : 0;
        // Compute digest for all queries
        if (conf.enableComputeAllQueryDigest && (event.digest == null || StringUtils.isBlank(event.digest))) {
            event.digest = computeStatementDigest(event.stmt);
            LOG.debug("compute stmt digest, queryId: {} digest: {}", event.queryId, event.digest);
        }
        String candidateMvsVal = candidateMvsExists ? event.candidateMvs : "";
        String hitMVsVal = hitMVsExists ? event.hitMVs : "";
        String content = "{\"queryId\":\"" + getQueryId(queryType, event) + "\"," +
                "\"timestamp\":\"" + longToTimeString(event.timestamp) + "\"," +
                "\"queryType\":\"" + queryType + "\"," +
                "\"clientIp\":\"" + event.clientIp + "\"," +
                "\"user\":\"" + event.user + "\"," +
                "\"authorizedUser\":\"" + event.authorizedUser + "\"," +
                "\"resourceGroup\":\"" + event.resourceGroup + "\"," +
                "\"catalog\":\"" + event.catalog + "\"," +
                "\"db\":\"" + event.db + "\"," +
                "\"state\":\"" + event.state + "\"," +
                "\"errorCode\":\"" + event.errorCode + "\"," +
                "\"queryTime\":" + event.queryTime + "," +
                "\"scanBytes\":" + event.scanBytes + "," +
                "\"scanRows\":" + event.scanRows + "," +
                "\"returnRows\":" + event.returnRows + "," +
                "\"cpuCostNs\":" + event.cpuCostNs + "," +
                "\"memCostBytes\":" + event.memCostBytes + "," +
                "\"stmtId\":" + event.stmtId + "," +
                "\"isQuery\":" + isQuery + "," +
                "\"feIp\":\"" + event.feIp + "\"," +
                "\"stmt\":\"" + truncateByBytes(event.stmt) + "\"," +
                "\"digest\":\"" + event.digest + "\"," +
                "\"planCpuCosts\":" + event.planCpuCosts + "," +
                "\"planMemCosts\":" + event.planMemCosts + "," +
                "\"pendingTimeMs\":" + event.pendingTimeMs + "," +
                "\"candidateMVs\":\"" + candidateMvsVal + "\"," +
                "\"hitMvs\":\"" + hitMVsVal + "\"}";
        if (auditBuffer.length() > 0) {
            auditBuffer.append(",");
        }
        auditBuffer.append(content);
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

    private String computeStatementDigest(String stmt) {
        List<StatementBase> stmts = SqlParser.parse(stmt, 32);
        StatementBase queryStmt = stmts.get(stmts.size() - 1);

        if (queryStmt == null) {
            return "";
        }
        String digest = SqlDigestBuilder.build(queryStmt);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(digest.getBytes());
            return Hex.encodeHexString(md.digest());
        } catch (NoSuchAlgorithmException | NullPointerException e) {
            return "";
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

    private void loadIfNecessary(StarrocksStreamLoader loader) {
        if (auditBuffer.length() < conf.maxBatchSize && System.currentTimeMillis() - lastLoadTime < conf.maxBatchIntervalSec * 1000) {
            return;
        }
        if (auditBuffer.length() == 0) {
            return;
        }

        lastLoadTime = System.currentTimeMillis();
        // begin to load
        try {
            StarrocksStreamLoader.LoadResponse response = loader.loadBatch(auditBuffer);
            LOG.debug("audit loader response: {}", response);
        } catch (Exception e) {
            LOG.error("encounter exception when putting current audit batch, discard current batch", e);
        } finally {
            // make a new string builder to receive following events.
            this.auditBuffer = new StringBuilder();
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
        public static final String ENABLE_COMPUTE_ALL_QUERY_DIGEST = "enable_compute_all_query_digest";

        public static final String STREAM_LOAD_FILTER = "filter";

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

        public boolean enableComputeAllQueryDigest = false;
        public String streamLoadFilter = "";

        public static final String PROP_SECRET_KEY = "secret_key";
        public String secretKey = "";

        public static String PROP_KAFKA_ENABLE = "kafka_enableMSK";
        public static String PROP_KAFKA_INSTANCE_NAME = "kafka_instanceName";
        public static String PROP_KAFKA_TOPIC = "kafka_topic";
        public static String PROP_KAFKA_BOOTSTRAP_SERVERS = "kafka_bootstrapServer";
        public static String PROP_KAFKA_CONF_KEYSERIALIZER = "kafka_conf_keySerializer";
        public static String PROP_KAFKA_CONF_VALUESERIALIZER = "kafka_conf_valueSerializer";
        public static String PROP_KAFKA_CONF_MAXREQUESTSIZE = "kafka_conf_maxRequestSize";
        public static String PROP_KAFKA_CONF_BUFFERMEMORY = "kafka_conf_bufferMemory";
        public static String PROP_KAFKA_CONF_MAXBLOCKMS = "kafka_conf_maxBlockMs";
        public static String PROP_KAFKA_CONF_BATCHSIZE = "kafka_conf_batchSize";
        public static String PROP_KAFKA_CONF_COMPRESSIONTYPE = "kafka_conf_compressionType";
        public static String PROP_KAFKA_CONF_LINGERMS = "kafka_conf_lingerMs";
        public static String PROP_KAFKA_CONF_SECURITYPROTOCOL = "kafka_conf_securityProtocol";
        public static String PROP_KAFKA_CONF_SASLMECHANISM = "kafka_conf_saslMechanism";
        public static String PROP_KAFKA_CONF_SASLJAASCONFIG = "kafka_conf_saslJaasConfig";
        public static String PROP_KAFKA_CONF_SASLCLIENTCALLBACKHANDLERCLASS = "kafka_conf_saslClientCallbackHandlerClass";

        public void init(Map<String, String> properties) throws PluginException {
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
                if (properties.containsKey(ENABLE_COMPUTE_ALL_QUERY_DIGEST)) {
                    enableComputeAllQueryDigest = Boolean.parseBoolean(properties.get(ENABLE_COMPUTE_ALL_QUERY_DIGEST));
                }
                if (properties.containsKey(STREAM_LOAD_FILTER)) {
                    streamLoadFilter = properties.get(STREAM_LOAD_FILTER);
                }
                if (properties.containsKey(PROP_KAFKA_ENABLE)) {
                    kafkaEnable = Boolean.parseBoolean(properties.get(PROP_KAFKA_ENABLE));
                }
                if (properties.containsKey(PROP_KAFKA_INSTANCE_NAME)) {
                    PROP_KAFKA_INSTANCE_NAME = properties.get(PROP_KAFKA_INSTANCE_NAME);
                }
                if (properties.containsKey(PROP_KAFKA_TOPIC)) {
                    PROP_KAFKA_TOPIC = properties.get(PROP_KAFKA_TOPIC);
                }
                if (properties.containsKey(PROP_KAFKA_BOOTSTRAP_SERVERS)) {
                    PROP_KAFKA_BOOTSTRAP_SERVERS = properties.get(PROP_KAFKA_BOOTSTRAP_SERVERS);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_KEYSERIALIZER)) {
                    PROP_KAFKA_CONF_KEYSERIALIZER = properties.get(PROP_KAFKA_CONF_KEYSERIALIZER);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_VALUESERIALIZER)) {
                    PROP_KAFKA_CONF_VALUESERIALIZER = properties.get(PROP_KAFKA_CONF_VALUESERIALIZER);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_MAXREQUESTSIZE)) {
                    PROP_KAFKA_CONF_MAXREQUESTSIZE = properties.get(PROP_KAFKA_CONF_MAXREQUESTSIZE);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_BUFFERMEMORY)) {
                    PROP_KAFKA_CONF_BUFFERMEMORY = properties.get(PROP_KAFKA_CONF_BUFFERMEMORY);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_MAXBLOCKMS)) {
                    PROP_KAFKA_CONF_MAXBLOCKMS = properties.get(PROP_KAFKA_CONF_MAXBLOCKMS);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_BATCHSIZE)) {
                    PROP_KAFKA_CONF_BATCHSIZE = properties.get(PROP_KAFKA_CONF_BATCHSIZE);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_COMPRESSIONTYPE)) {
                    PROP_KAFKA_CONF_COMPRESSIONTYPE = properties.get(PROP_KAFKA_CONF_COMPRESSIONTYPE);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_LINGERMS)) {
                    PROP_KAFKA_CONF_LINGERMS = properties.get(PROP_KAFKA_CONF_LINGERMS);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_SECURITYPROTOCOL)) {
                    PROP_KAFKA_CONF_SECURITYPROTOCOL = properties.get(PROP_KAFKA_CONF_SECURITYPROTOCOL);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_SASLMECHANISM)) {
                    PROP_KAFKA_CONF_SASLMECHANISM = properties.get(PROP_KAFKA_CONF_SASLMECHANISM);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_SASLJAASCONFIG)) {
                    PROP_KAFKA_CONF_SASLJAASCONFIG = properties.get(PROP_KAFKA_CONF_SASLJAASCONFIG);
                }
                if (properties.containsKey(PROP_KAFKA_CONF_SASLCLIENTCALLBACKHANDLERCLASS)) {
                    PROP_KAFKA_CONF_SASLCLIENTCALLBACKHANDLERCLASS = properties.get(PROP_KAFKA_CONF_SASLCLIENTCALLBACKHANDLERCLASS);
                }
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        private StarrocksStreamLoader loader;

        public LoadWorker(StarrocksStreamLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                        if (kafkaEnable) {
                            sendToKafka(event);
                        }
                    }
                    loadIfNecessary(loader);
                } catch (InterruptedException ie) {
                    LOG.debug("encounter exception when loading current audit batch", ie);
                } catch (Exception e) {
                    LOG.error("run audit logger error:", e);
                }
            }
        }
    }

    public static synchronized String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return DATETIME_FORMAT.format(new Date());
        }
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }

    public void sendToKafka(AuditEvent event){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", AuditLoaderConf.PROP_KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty("key.serializer",  AuditLoaderConf.PROP_KAFKA_CONF_KEYSERIALIZER);
        properties.setProperty("value.serializer",  AuditLoaderConf.PROP_KAFKA_CONF_VALUESERIALIZER);
        properties.setProperty("max.request.size", AuditLoaderConf.PROP_KAFKA_CONF_MAXREQUESTSIZE);
        properties.setProperty("buffer.memory", AuditLoaderConf.PROP_KAFKA_CONF_BUFFERMEMORY);
        properties.setProperty("max.block.ms", AuditLoaderConf.PROP_KAFKA_CONF_MAXBLOCKMS);
        properties.setProperty("batch.size", AuditLoaderConf.PROP_KAFKA_CONF_BATCHSIZE);
        properties.setProperty("compression.type", AuditLoaderConf.PROP_KAFKA_CONF_COMPRESSIONTYPE);
        properties.setProperty("linger.ms", AuditLoaderConf.PROP_KAFKA_CONF_LINGERMS);
        properties.setProperty("security.protocol",AuditLoaderConf.PROP_KAFKA_CONF_SECURITYPROTOCOL);
        properties.setProperty("sasl.mechanism",AuditLoaderConf.PROP_KAFKA_CONF_SASLMECHANISM);
        properties.setProperty("sasl.jaas.config",AuditLoaderConf.PROP_KAFKA_CONF_SASLJAASCONFIG);
        properties.setProperty("sasl.client.callback.handler.class",AuditLoaderConf.PROP_KAFKA_CONF_SASLCLIENTCALLBACKHANDLERCLASS);

        String queryType = getQueryType(event);
        String eventAuditId = getQueryId(queryType,event);

        QueryEventData eventAuditEG = new QueryEventData();
        eventAuditEG.setId(eventAuditId);
        eventAuditEG.setInstanceName(AuditLoaderConf.PROP_KAFKA_INSTANCE_NAME);
        eventAuditEG.setTimestamp(longToTimeString(event.timestamp));
        eventAuditEG.setQueryType(queryType);
        eventAuditEG.setClientIp(event.clientIp);
        eventAuditEG.setUser(event.user);
        eventAuditEG.setAuthorizedUser(event.authorizedUser);
        eventAuditEG.setResourceGroup(event.resourceGroup);
        eventAuditEG.setCatalog(event.catalog);
        eventAuditEG.setDb(event.db);
        eventAuditEG.setState(event.state);
        eventAuditEG.setErrorCode(event.errorCode);
        eventAuditEG.setQueryTime(event.queryTime);
        eventAuditEG.setScanBytes(event.scanBytes);
        eventAuditEG.setScanRows(event.scanRows);
        eventAuditEG.setReturnRows(event.returnRows);
        eventAuditEG.setCpuCostNs(event.cpuCostNs);
        eventAuditEG.setMemCostBytes(event.memCostBytes);
        eventAuditEG.setStmtId(event.stmtId);
        eventAuditEG.setIsQuery(event.isQuery ? true : false);
        eventAuditEG.setFeIp(event.feIp);
        eventAuditEG.setStmt(truncateByBytes(event.stmt));
        if (conf.enableComputeAllQueryDigest && (event.digest == null || StringUtils.isBlank(event.digest))) {
            event.digest = computeStatementDigest(event.stmt);
            LOG.debug("compute stmt digest, queryId: {} digest: {}", event.queryId, event.digest);
        }
        eventAuditEG.setDigest(event.digest);
        eventAuditEG.setPlanCpuCosts(event.planCpuCosts);
        eventAuditEG.setPlanMemCosts(event.planMemCosts);
        eventAuditEG.setCandidateMvs(event.candidateMvs);
        eventAuditEG.setHitMVs(event.hitMVs);

        ObjectMapper mapperEventAuditEG = new ObjectMapper();
        mapperEventAuditEG.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        try {
            Producer<String, String> producer = new KafkaProducer<>(properties);
            Future<RecordMetadata> res = producer.send(
                new ProducerRecord<>(
                    AuditLoaderConf.PROP_KAFKA_TOPIC, 
                    eventAuditId, 
                    mapperEventAuditEG.writeValueAsString(eventAuditEG)));
            try {
                RecordMetadata metadata = res.get();
                if (metadata.hasOffset()){
                    LOG.info("Query created event with id: " + eventAuditId +  " in partition: "+ String.valueOf(metadata.partition())  + " with offset: " + metadata.offset());
                } else {
                    LOG.error("Query created event with id: " + eventAuditId +  " doesn't have offset. It wasn't sent to the topic. ");
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(String.format("Query id: "+ eventAuditId + " Not written to kafka topic - Error of interrupted execution on sendToKafka method: %s", e.getMessage()));
            }
            producer.close();
        } catch (Exception e) {
            LOG.error(String.format("Error on sending to kafka: %s", e.getMessage()));
        }

    }

}
