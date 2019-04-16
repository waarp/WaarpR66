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


package org.waarp.openr66.protocol.http.restv2.converters;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;

import javax.ws.rs.InternalServerErrorException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.restv2.converters.HostConverter.FieldNames.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.FIELD_NOT_ALLOWED;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_FIELD_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.MISSING_FIELD;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.UNKNOWN_FIELD;

/**
 * A collection of utility methods to convert {@link Host} POJOs
 * to {@link ObjectNode} and vice-versa.
 */
public final class HostConverter {

    @SuppressWarnings("unused")
    public static final class FieldNames {
        public static final String HOST_NAME = "name";
        public static final String ADDRESS = "address";
        public static final String PORT = "port";
        public static final String PASSWORD = "password";
        public static final String IS_SSL = "isSSL";
        public static final String IS_CLIENT = "isClient";
        public static final String IS_ADMIN = "isAdmin";
        public static final String IS_ACTIVE = "isActive";
        public static final String IS_PROXY = "isProxy";
    }

    /** Represents all the possible ways to order a list of host objects. */
    public enum Order {
        /** By hostID, in ascending order. */
        ascId(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.getHostid().compareTo(t2.getHostid());
            }
        }),
        /** By hostID, in descending order. */
        descId(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.getHostid().compareTo(t2.getHostid());
            }
        }),
        /** By address, in ascending order. */
        ascAddress(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.getAddress().compareTo(t2.getAddress());
            }
        }),
        /** By address, in descending order. */
        descAddress(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.getAddress().compareTo(t2.getAddress());
            }
        });

        /** The comparator used to sort the list of RestHost objects. */
        public final Comparator<Host> comparator;

        Order(Comparator<Host> comparator) {
            this.comparator = comparator;
        }
    }

    private static byte[] encryptPassword(String password) {
        try {
            return configuration.getCryptoKey().cryptToHex(password).getBytes();
        } catch (Exception e) {
            throw new InternalServerErrorException("Failed to encrypt the host password", e);
        }
    }

    private static List<Error> checkRequiredFields(Host host) {
        List<Error> errors = new ArrayList<Error>();
        if (host.getHostid() == null || host.getHostid().isEmpty()) {
            errors.add(MISSING_FIELD(HOST_NAME));
        }
        if (host.getAddress() == null || host.getAddress().isEmpty()) {
            errors.add(MISSING_FIELD(ADDRESS));
        }
        if (host.getPort() == -1) {
            errors.add(MISSING_FIELD(PORT));
        }
        if (host.getHostkey() == null || host.getHostkey().length == 0) {
            errors.add(MISSING_FIELD(PASSWORD));
        }

        return errors;
    }

    public static ObjectNode hostToNode(Host host) {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(HOST_NAME, host.getHostid());
        node.put(ADDRESS, host.getAddress());
        node.put(PORT, host.getPort());
        node.put(PASSWORD, host.getHostkey());
        node.put(IS_SSL, host.isSSL());
        node.put(IS_CLIENT, host.isClient());
        node.put(IS_ADMIN, host.isAdmin());
        node.put(IS_ACTIVE, host.isActive());
        node.put(IS_PROXY, host.isProxified());

        return node;
    }

    public static Host nodeToNewHost(ObjectNode object)
            throws UserErrorException {
        Host defaultHost = new Host(null, null, -1, null, false, false, false, false, true);

        return parseNode(object, defaultHost);
    }

    public static Host nodeToUpdatedHost(ObjectNode object, Host oldHost)
            throws UserErrorException {

        return parseNode(object, oldHost);
    }

    private static Host parseNode(ObjectNode object, Host host)
            throws UserErrorException {
        List<Error> errors = new ArrayList<Error>();

        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();

            if (name.equalsIgnoreCase(HOST_NAME)) {
                if (value.isTextual()) {
                    if (host.getHostid() == null) {
                        host.setHostid(value.asText());
                    } else if (!host.getHostid().equals(value.asText())) {
                        errors.add(FIELD_NOT_ALLOWED(HOST_NAME));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(ADDRESS)) {
                if (value.isTextual()) {
                    host.setAddress(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(PORT)) {
                if (value.canConvertToInt() && value.asInt() >= 0 && value.asInt() < 65536) {
                    host.setPort(value.asInt());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(PASSWORD)) {
                if (value.isTextual()) {
                    host.setHostkey(encryptPassword(value.asText()));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(IS_SSL)) {
                if (value.isBoolean()) {
                    host.setSSL(value.asBoolean());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(IS_CLIENT)) {
                if (value.isBoolean()) {
                    host.setClient(value.asBoolean());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(IS_ADMIN)) {
                if (value.isBoolean()) {
                    host.setAdmin(value.asBoolean());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(IS_ACTIVE)) {
                if (value.isBoolean()) {
                    host.setActive(value.asBoolean());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(IS_PROXY)) {
                if (value.isBoolean()) {
                    host.setProxified(value.asBoolean());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else {
                errors.add(UNKNOWN_FIELD(name));
            }
        }

        errors.addAll(checkRequiredFields(host));

        if (errors.isEmpty()) {
            return host;
        } else {
            throw new UserErrorException(errors);
        }
    }

    /** Prevents the default constructor from being called. */
    private HostConverter() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }
}
