/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */


package org.waarp.openr66.protocol.http.restv2.data;


import java.util.Comparator;

/**
 * Host POJO
 * for Rest HTTP support for R66.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Host {

    /** All the possible ways to order a list of host objects. */
    public enum Order {
        /** By hostID, in ascending order. */
        ascHostID("+id", new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By hostID, in descending order. */
        descHostID("-id", new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By address, in ascending order. */
        ascAddress("+address", new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.address.compareTo(t2.address);
            }
        }),
        /** By address, in descending order. */
        descAddress("-address", new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.address.compareTo(t2.address);
            }
        });

        public final Comparator<Host> comparator;
        public final String value;

        Order(String value, Comparator<Host> comparator) {
            this.value = value;
            this.comparator = comparator;
        }

        @Override
        public String toString(){
            return this.value;
        }

        public static Order fromString(String str) throws InstantiationException {
            if(str == null || str.isEmpty()) {
                return ascHostID;
            }
            else {
                for(Order order : Order.values()) {
                    if(order.value.equals(str)) {
                        return order;
                    }
                }
                throw new InstantiationException();
            }
        }
    }

    /** Initializes all missing optional fields with their default values. */
    public void defaultValues() {
        if(this.isSSL == null)       this.isSSL = false;
        if(this.adminRole == null)   this.adminRole = false;
        if(this.isClient == null)    this.isClient = false;
        if(this.isActive == null)    this.isActive = false;
        if(this.isProxyfied == null) this.isProxyfied = false;
    }

    /** The host's unique identifier. */
    public String hostID;

    /** The host's public address. Can be an IP address, or a web address which will then be resolved by DNS. */
    public String address;

    /** The server's listening port. Must be between -1 and 65535. */
    public Integer port;

    /** The host's DES encrypted password. DO NOT store the password in plaintext. */
    public String hostKey;

    /** If true, this host will accept SSL mode connections. */
    public Boolean isSSL;

    /** If true, the host will have admin access on other servers. */
    public Boolean adminRole;

    /** If true, this host is a client, and thus will not accept any incoming connections. */
    public Boolean isClient;

    /** If true, this host is currently active. */
    public Boolean isActive;

    /** If true, the address field is actually the address of the proxy used by this host. */
    public Boolean isProxyfied;
}
