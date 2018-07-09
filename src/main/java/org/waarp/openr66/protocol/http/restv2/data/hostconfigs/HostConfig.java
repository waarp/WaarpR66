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

import java.util.List;
import java.util.Map;


/** Host configuration JSON object for Rest HTTP support for R66. */
public final class HostConfig {

    /** All the different actions a host is allowed to perform on the server running this configuration. */
    public enum Role {
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

    /** The host of the host using this configuration. */
    public String hostId;

    /** The list of al hosts allowed to make request to execute the server's business. */
    public List<String> business;

    /**
     * The list of all hosts paired with the list of actions they are each allowed to perform on the server.
     *
     * @see Role
     */
    public List<Map.Entry<String, List<Role>>> roles;

    /** The list of all the server's aliases. */
    public List<String> aliases;

    /** The database configuration version in XML format. */
    public String others;
}
