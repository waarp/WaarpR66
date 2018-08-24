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


import org.waarp.openr66.pojo.Host;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * RestHost POJO
 * for Rest HTTP support for R66.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class RestHost {

    public RestHost() {}

    public RestHost(Host host) {
        this.hostID = host.getHostid();
        this.address = host.getAddress();
        this.port = host.getPort();
        this.hostKey = new String(host.getHostkey());
        this.isSSL = host.isSSL();
        this.adminRole = host.isAdmin();
        this.isClient = host.isClient();
        this.isActive = host.isActive();
        this.isProxyfied = host.isProxified();
    }

    /** All the possible ways to order a list of host objects. */
    public enum Order {
        /** By hostID, in ascending order. */
        ascHostID("+id", new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By hostID, in descending order. */
        descHostID("-id", new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return -t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By address, in ascending order. */
        ascAddress("+address", new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return t1.address.compareTo(t2.address);
            }
        }),
        /** By address, in descending order. */
        descAddress("-address", new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return -t1.address.compareTo(t2.address);
            }
        });

        public final Comparator<RestHost> comparator;
        public final String value;

        Order(String value, Comparator<RestHost> comparator) {
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

    /** The host's unique identifier. */
    @NotEmpty
    public String hostID;

    /** The host's public address. Can be an IP address, or a web address which will then be resolved by DNS. */
    @NotEmpty
    public String address;

    /** The server's listening port. Must be between -1 and 65535. */
    @Or(value = {@Bounds(min = Long.MIN_VALUE, max = -1L), @Bounds(min = 1, max = Long.MAX_VALUE)})
    public Integer port;

    /** The host's DES encrypted password. DO NOT store the password in plaintext. */
    @NotEmpty
    public String hostKey;

    /** If true, this host will accept SSL mode connections. */
    public Boolean isSSL = false;

    /** If true, the host will have admin access on other servers. */
    public Boolean adminRole = false;

    /** If true, this host is a client, and thus will not accept any incoming connections. */
    public Boolean isClient = false;

    /** If true, this host is currently active. */
    public Boolean isActive = false;

    /** If true, the address field is actually the address of the proxy used by this host. */
    public Boolean isProxyfied = false;



    public static List<RestHost> toRestList(List<Host> hosts) {
        List<RestHost> restHosts = new ArrayList<RestHost>();
        for(Host host : hosts) {
            restHosts.add(new RestHost(host));
        }
        return restHosts;
    }

    public Host toHost() {
        return new Host(this.hostID, this.address, this.port, this.hostKey.getBytes(),
                this.isSSL, this.isClient, this.isProxyfied, this.adminRole, this.isActive);
    }
}
