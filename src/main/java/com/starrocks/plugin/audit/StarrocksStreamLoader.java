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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;
import java.util.UUID;

public class StarrocksStreamLoader {
    private final static Logger LOG = LogManager.getLogger(StarrocksStreamLoader.class);
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private static Integer TEMPORARY_REDIRECT_CODE = 307;
    private String hostPort;
    private String db;
    private String tbl;
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String authEncoding;
    private String feIdentity;

    private int connectTimeout;
    private int readTimeout;

    private String streamLoadFilter;

    public StarrocksStreamLoader(AuditLoaderPlugin.AuditLoaderConf conf) {
        this.hostPort = conf.frontendHostPort;
        this.db = conf.database;
        this.tbl = conf.table;
        this.user = conf.user;
        this.passwd = conf.password;

        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        String secretKey = conf.secretKey;
        if(!StringUtils.isBlank(secretKey.trim())) {
            try {
                this.passwd = DecryptUtil.decrypt(this.passwd, DecryptUtil.fillByte(secretKey.getBytes(), 16));
            } catch (Exception e) {
                LOG.error("AuditLoader plugin decrypt password error ", e);
            }
        }
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        // currently, FE identity is FE's IP, so we replace the "." in IP to make it suitable for label
        this.feIdentity = conf.feIdentity.replaceAll("\\.", "_");

        this.connectTimeout = conf.connectTimeout;
        this.readTimeout = conf.readTimeout;
      
        this.streamLoadFilter = conf.streamLoadFilter;
    }

    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("format", "json");
        conn.addRequestProperty("strip_outer_array", "true");

        conn.addRequestProperty("label", label);
        conn.addRequestProperty("max_filter_ratio", "1.0");
        conn.addRequestProperty("columns", "queryId,timestamp,queryType,clientIp,user,authorizedUser,resourceGroup,catalog,db,state,errorCode,queryTime,scanBytes,scanRows,returnRows,cpuCostNs,memCostBytes,stmtId,isQuery,feIp,stmt,digest,planCpuCosts,planMemCosts,pendingTimeMs,candidateMVs,hitMvs,warehouse");
        if(!StringUtils.isBlank(this.streamLoadFilter)) {
            conn.addRequestProperty("where", streamLoadFilter);
        }

        conn.setDoOutput(true);
        conn.setDoInput(true);

        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);

        return conn;
    }

    private String toCurl(HttpURLConnection conn) {
        StringBuilder sb = new StringBuilder("curl -v ");
        sb.append("-X ").append(conn.getRequestMethod()).append(" \\\n  ");
        sb.append("-H \"").append("Authorization\":").append("\"Basic " + authEncoding).append("\" \\\n  ");
        sb.append("-H \"").append("Expect\":").append("\"100-continue\" \\\n  ");
        sb.append("-H \"").append("Content-Type\":").append("\"text/plain; charset=UTF-8\" \\\n  ");
        sb.append("-H \"").append("format\":").append("\"json \\\n  ");
        sb.append("-H \"").append("strip_outer_array\":").append("\"true \\\n  ");
        if(!StringUtils.isBlank(this.streamLoadFilter)) {
            sb.append("-H \"").append("where\":").append(streamLoadFilter).append(" \\\n  ");
        }
        sb.append("-H \"").append("columns\":").append("\"queryId, timestamp, queryType, clientIp, user, authorizedUser, resourceGroup, catalog, db, state, errorCode," +
                "queryTime, scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, stmtId, isQuery, feIp, stmt, digest, planCpuCosts, planMemCosts, pendingTimeMs, candidateMVs, hitMvs, warehouse\" \\\n  ");
        sb.append("\"").append(conn.getURL()).append("\"");
        return sb.toString();
    }

    private String getContent(HttpURLConnection conn) {
        BufferedReader br = null;
        StringBuilder response = new StringBuilder();
        String line;
        try {
            if (100 <= conn.getResponseCode() && conn.getResponseCode() <= 399) {
                br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            } else {
                br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            }
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
        } catch (IOException e) {
            LOG.warn("get content error,", e);
        }

        return response.toString();
    }

    public LoadResponse loadBatch(StringBuilder sb) {
        Calendar calendar = Calendar.getInstance();
        // label length limit is less than 128 , audit_%s%02d%02d_%02d%02d%02d_ length is 22
        String labelId = this.feIdentity.length() > 106 ? UUID.randomUUID().toString().replaceAll("-","") : this.feIdentity;
        String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                labelId);

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            // build request and send to fe
            feConn = getConnection(loadUrlStr, label);
            // print curl load command in fe.log
            // LOG.info(toCurl(feConn));
            int status = feConn.getResponseCode();
            // fe send back http response code TEMPORARY_REDIRECT 307 and new be location, or response code HTTP_OK 200 from nginx
            if (status != TEMPORARY_REDIRECT_CODE && status != HttpURLConnection.HTTP_OK) {
                throw new Exception("status is not TEMPORARY_REDIRECT 307 or HTTP_OK 200, status: " + status
                        + ", response: " + getContent(feConn) + ", request is: " + toCurl(feConn));
            }
            String location = feConn.getHeaderField("Location");
            if (status == TEMPORARY_REDIRECT_CODE && location == null) {
                throw new Exception("redirect location is null");
            }
            // build request and send to new be location, or use old conn if status is 200
            beConn = status == TEMPORARY_REDIRECT_CODE ? getConnection(location, label) : getConnection(loadUrlStr, label);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            String content = "[" + sb.toString() + "]";
            bos.write(content.getBytes());
            bos.close();

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            String response = getContent(beConn);

            LOG.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}",
                    label, status, respMsg, response);

            return new LoadResponse(status, respMsg, response);

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if (feConn != null) {
                feConn.disconnect();
            }
            if (beConn != null) {
                beConn.disconnect();
            }
        }
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("status: ").append(status);
            sb.append(", resp msg: ").append(respMsg);
            sb.append(", resp content: ").append(respContent);
            return sb.toString();
        }
    }
}
