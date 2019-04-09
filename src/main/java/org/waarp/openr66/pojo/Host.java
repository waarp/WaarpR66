package org.waarp.openr66.pojo;

/**
 * Host data object
 */
public class Host {
    private static final String DEFAULT_CLIENT_ADDRESS = "0.0.0.0";
    private static final int DEFAULT_CLIENT_PORT = 0;

    private String hostid;

    private String address;

    private int port;

    private byte[] hostkey;

    private boolean ssl;

    private boolean client;

    private boolean proxified = false;

    private boolean admin = true;

    private boolean active = true;

    private UpdatedInfo updatedInfo = UpdatedInfo.UNKNOWN;

    /**
     * Empty constructor for compatibility issues
     */
    @Deprecated
    public Host() {}
    
    public Host(String hostid, String address, int port, byte[] hostkey,
        boolean ssl, boolean client, boolean proxified, boolean admin, 
        boolean active, UpdatedInfo updatedInfo) {
        this(hostid, address, port, hostkey, ssl, 
                client, proxified, admin, active);
        this.updatedInfo = updatedInfo;
    }

    public Host(String hostid, String address, int port, byte[] hostkey,
            boolean ssl, boolean client, boolean proxified, boolean admin, 
            boolean active) {
        this.hostid = hostid;
        this.hostkey = hostkey;
        //Force client status if unvalid port
        if (port < 1) {
            this.address = DEFAULT_CLIENT_ADDRESS;
            this.port = DEFAULT_CLIENT_PORT;
            this.client = true;
        } else {
            this.address = address;
            this.port = port;
            this.client = client;
        }
        this.ssl = ssl;
        this.proxified = proxified;
        this.admin = admin;
        this.active = active;
    }

    public Host(String hostid, String address, int port, byte[] hostkey,
            boolean ssl, boolean client, boolean proxified, boolean admin) { 
        this(hostid, address, port, hostkey, ssl, 
                client, proxified, admin, true);
    }

    public Host(String hostid, String address, int port, byte[] hostkey,
            boolean ssl, boolean client) {
        this(hostid, address, port, hostkey, ssl, client, false, true);
    }

    public String getHostid() {
        return this.hostid;
    }

    public void setHostid(String hostid) {
        this.hostid = hostid;
    }

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public byte[] getHostkey() {
        return this.hostkey;
    }

    public void setHostkey(byte[] hostkey) {
        this.hostkey = hostkey;
    }

    public boolean isSSL() {
        return this.ssl;
    }

    public void setSSL(boolean ssl) {
        this.ssl = ssl;
    }

    public boolean isClient() {
        return this.client;
    }

    public void setClient(boolean client) {
        this.client = client;
    }

    public boolean isAdmin() {
        return this.admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean isProxified() {
        return this.proxified;
    }

    public void setProxified(boolean proxified) {
        this.proxified = proxified;
    }

    public boolean isActive() {
        return this.active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public UpdatedInfo getUpdatedInfo() {
        return this.updatedInfo;
    }

    public void setUpdatedInfo(UpdatedInfo info) {
        this.updatedInfo = info;
    }
}
