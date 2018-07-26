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

package org.waarp.openr66.protocol.http.restv2.data.hostconfigs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import static org.waarp.openr66.protocol.http.restv2.RestUtils.HOST_ID;


/** Host configuration JSON object for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class HostConfig {

    /** All the different actions a host is allowed to perform on the server running this configuration. */
    public enum RoleType {
        /** No actions are allowed. */
        noAccess,
        /** The host is ony allowed to read the database. */
        readOnly,
        /** The host is allowed to create transfers. */
        transfer,
        /** The host is allowed to create and modify transfer rules. */
        rule,
        /** The host is allowed to create and modify host entries. */
        host,
        /** The host is allowed to create and modify bandwidth limits. */
        limit,
        /** The host is allowed to deactivate or reboot the server. */
        system,
        /** The host is allowed to export the server logs. */
        logControl,
        /** The host is allowed to perform all the 'readOnly' and 'transfer' actions. */
        partner,
        /** The host is allowed to perform all the 'partner', 'rule' and 'host' actions. */
        configAdmin,
        /** The host is allowed to perform all actions on the server. */
        fullAdmin
    }

    /** A pair associating a host with the type of actions it is allowed to perform on the server. */
    public static class Role {
        /** The host's id. */
        public String host;

        /** The list of allowed actions on the server. */
        public RoleType[] roleTypes;

        /**
         * Constructs a new role from a host and a list of actions.
         *
         * @param host      The host's id.
         * @param roleTypes The host's allowed actions.
         */
        public Role(String host, RoleType[] roleTypes) {
            this.host = host;
            this.roleTypes = roleTypes;
        }
    }

    public static class OptionalHostConfig extends HostConfig {
        public OptionalHostConfig() {
            this.business = new String[0];
            this.roles = new Role[0];
            this.aliases = new String[0];
            this.others = "";
        }
    }

    /** The host of the host using this configuration. */
    @JsonIgnore
    public final String hostId = HOST_ID;

    /** The list of al hosts allowed to make request to execute the server's business. */
    public String[] business;

    /**
     * The list of all hosts paired with the list of actions they are each allowed to perform on the server.
     *
     * @see RoleType
     */
    public Role[] roles;

    /** The list of all the server's aliases. */
    public String[] aliases;

    /** The database configuration version in XML format. */
    public String others;
}
