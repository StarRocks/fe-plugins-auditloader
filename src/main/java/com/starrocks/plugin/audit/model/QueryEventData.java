package com.starrocks.plugin.audit.model;

public class QueryEventData {

    public String id;
    public String instanceName;
    public String timestamp;
    public String queryType;
    public String clientIp;
    public String user;
    public String authorizedUser;
    public String resourceGroup;
    public String catalog;
    public String db;
    public String state;
    public String errorCode;
    public long queryTime;
    public long scanBytes;
    public long scanRows;
    public long returnRows;
    public long cpuCostNs;
    public long memCostBytes;
    public long stmtId;
    public boolean isQuery;
    public String feIp;
    public String stmt;
    public String digest;
    public double planCpuCosts;
    public double planMemCosts;
    public String candidateMvs;
    public String hitMVs;
    
    public QueryEventData() {

    }

    public void setId(String id) {
        this.id = id;
    }
    
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setAuthorizedUser(String authorizedUser) {
        this.authorizedUser = authorizedUser;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setQueryTime(long queryTime) {
        this.queryTime = queryTime;
    }

    public void setScanBytes(long scanBytes) {
        this.scanBytes = scanBytes;
    }

    public void setScanRows(long scanRows) {
        this.scanRows = scanRows;
    }

    public void setReturnRows(long returnRows) {
        this.returnRows = returnRows;
    }

    public void setCpuCostNs(long cpuCostNs) {
        this.cpuCostNs = cpuCostNs;
    }

    public void setMemCostBytes(long memCostBytes) {
        this.memCostBytes = memCostBytes;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public void setFeIp(String feIp) {
        this.feIp = feIp;
    }

    public void setStmt(String stmt) {
        this.stmt = stmt;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public void setPlanCpuCosts(double planCpuCosts) {
        this.planCpuCosts = planCpuCosts;
    }

    public void setPlanMemCosts(double planMemCosts) {
        this.planMemCosts = planMemCosts;
    }

    public void setCandidateMvs(String candidateMvs) {
        this.candidateMvs = candidateMvs;
    }

    public void setHitMVs(String hitMVs) {
        this.hitMVs = hitMVs;
    }

    

}
