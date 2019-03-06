/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */


package org.waarp.openr66.protocol.http.restv2.data;


import org.waarp.openr66.pojo.Host;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.InternalServerErrorException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.waarp.openr66.protocol.configuration.Configuration.configuration;

/**
 * Defines a single host entry as represented in the REST API after*
 * deserialization of the host JSON object.
 */
@SuppressWarnings({"unused"})
@XmlType(name = "host")
public class RestHost {

    @XmlRootElement(name = "hosts")
    public static class RestHostList {
        @XmlElement(name = "host")
        public List<RestHost> hosts;
    }

    public RestHost() {}

    /**
     * Creates a RestHost instance from an existing {@link Host} instance.
     * This is used to convert a host entry from the database to an object that
     * can be serialised as a JSON.
     *
     * @param host The host POJO.
     */
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

    /** Represents all the possible ways to order a list of host objects. */
    public enum Order {
        /** By hostID, in ascending order. */
        ascHostID(new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By hostID, in descending order. */
        descHostID(new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return -t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By address, in ascending order. */
        ascAddress(new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return t1.address.compareTo(t2.address);
            }
        }),
        /** By address, in descending order. */
        descAddress(new Comparator<RestHost>() {
            @Override
            public int compare(RestHost t1, RestHost t2) {
                return -t1.address.compareTo(t2.address);
            }
        });

        /** The comparator used to sort the list of RestHost objects. */
        public final Comparator<RestHost> comparator;

        Order(Comparator<RestHost> comparator) {
            this.comparator = comparator;
        }
    }

    /** The host's unique identifier. */
    @Required
    @XmlElement
    public String hostID;

    /**
     * The host's public address. Can be an IP address, or a web address which
     * will then be resolved by DNS.
     */
    @Required
    @XmlElement
    public String address;

    /** The server's listening port. Must be between 0 and 65535. */
    @Bounds(min = 0, max = 65535)
    @XmlElement
    @Required
    public Integer port;

    /** The host's DES encrypted password. */
    @Required
    @XmlElement
    public String hostKey;

    /** If true, this host will accept SSL mode connections. */
    @XmlElement
    @DefaultValue("false")
    public Boolean isSSL;

    /** If true, the host will have admin access on other servers. */
    @XmlElement
    @DefaultValue("false")
    public Boolean adminRole;

    /** If true, the host will not accept any incoming transfer requests. */
    @XmlElement
    @DefaultValue("false")
    public Boolean isClient;

    /** If false, this host cannot participate in transfers. */
    @XmlElement
    @DefaultValue("true")
    public Boolean isActive;

    /** If true, this host is using a proxy instead of its' real address. */
    @XmlElement
    @DefaultValue("false")
    public Boolean isProxyfied;

    /**
     * Creates a list of {@code RestHost} objects from an existing list of
     * {@link Host} objects for serialization purposes.
     *
     * @param hosts The list of database host entries.
     * @return  The list of corresponding {@code RestHost} objects.
     */
    public static List<RestHost> toRestList(List<Host> hosts) {
        List<RestHost> restHosts = new ArrayList<RestHost>();
        for(Host host : hosts) {
            restHosts.add(new RestHost(host));
        }
        return restHosts;
    }

    public void encryptPassword() {
        try {
            this.hostKey = configuration.getCryptoKey().cryptToHex(this.hostKey);
        } catch (Exception e) {
            throw new InternalServerErrorException("Failed to encrypt the host password", e);
        }
    }

    /**
     * Creates a {@link Host} object equivalent to the current {@code RestHost}
     * instance.
     *
     * @return  The created database host entry.
     */
    public Host toHost() {
        return new Host(this.hostID, this.address, this.port,
                this.hostKey.getBytes(), this.isSSL, this.isClient,
                this.isProxyfied, this.adminRole, this.isActive);
    }
}
