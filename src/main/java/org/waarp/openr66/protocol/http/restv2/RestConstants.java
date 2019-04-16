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

package org.waarp.openr66.protocol.http.restv2;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.protocol.configuration.Configuration;

import java.nio.charset.Charset;

/**
 * List of all constants of the RESTv2 API. This includes the URI of all
 * the entry points, the name of HTTP headers specific to the API and
 * the DAO_FACTORY to create DAOs for the database.
 */
public final class RestConstants {

    /** Prevents the default constructor from being called. */
    private RestConstants() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    /** Name of the HTTP header used to store the user who made a request. */
    public static final String AUTH_USER = "X-Auth-User";
    /** Name of the HTTP header used to store the timestamp of the request. */
    public static final String AUTH_TIMESTAMP = "X-Auth-Timestamp";
    /** Name of the HTTP header used to store the signature key of a request. */
    public static final String AUTH_SIGNATURE = "X-Auth-Key";

    /**
     * Root directory of the API. All URIs for this version of the REST API
     * will be prefixed with this.
     */
    private static final String VERSION_PREFIX = "/v2/";
    /** Name of the URI parameter containing the id of an entry in a collection. */
    public static final String URI_ID = "id";
    /** Regex corresponding to the id URI parameter of an entry in a collection. */
    private static final String ID_PARAMETER = "/{"+ URI_ID + "}";
    /** Access point of the transfers collection. */
    public static final String TRANSFERS_HANDLER_URI = VERSION_PREFIX +
            "transfers/";
    /**
     * Access point of a single transfer entry. The {id} parameter refers to
     * the desired transfer's id.
     */
    public static final String TRANSFER_ID_HANDLER_URI = TRANSFERS_HANDLER_URI + ID_PARAMETER;
    /** Access point of the server commands. */
    public static final String SERVER_HANDLER_URI = VERSION_PREFIX + "server/";
    /** Access point of the transfer rules collection. */
    public static final String RULES_HANDLER_URI = VERSION_PREFIX + "rules/";
    /**
     * Access point of a single transfer rules entry. The {id} parameter refers
     * to the desired rule's id.
     */
    public static final String RULE_ID_HANDLER_URI = RULES_HANDLER_URI + ID_PARAMETER;
    /** Access point of the bandwidth limits. */
    public static final String LIMITS_HANDLER_URI = VERSION_PREFIX + "limits/";
    /** Access point of the hosts collection. */
    public static final String HOSTS_HANDLER_URI = VERSION_PREFIX + "hosts/";
    /**
     * Access point of a single host entry. The {id} parameter refers to
     * the desired host's id.
     */
    public static final String HOST_ID_HANDLER_URI = HOSTS_HANDLER_URI + ID_PARAMETER;
    /** Access point of the host configuration. */
    public static final String CONFIG_HANDLER_URI = VERSION_PREFIX + "hostconfig/";

    /** The name of this R66 server instance. */
    public static final String SERVER_NAME = Configuration.configuration.getHOST_ID();

    /** The DAO_FACTORY to generate connections to the underlying database. */
    public static final DAOFactory DAO_FACTORY;

    /** The UTF-8 {@link java.nio.charset.Charset} constant. */
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    static {
        DAOFactory.initialize(ConnectionFactory.getInstance());
        DAO_FACTORY = DAOFactory.getInstance();
    }
}
