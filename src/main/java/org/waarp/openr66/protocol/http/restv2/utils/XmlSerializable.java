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

package org.waarp.openr66.protocol.http.restv2.utils;

import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.configuration.RuleFileBasedConfiguration;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.RuleTask;
import org.waarp.openr66.pojo.Transfer;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlList;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

import static org.waarp.openr66.configuration.AuthenticationFileBasedConfiguration.XML_AUTHENTIFICATION_ENTRY;
import static org.waarp.openr66.configuration.AuthenticationFileBasedConfiguration.XML_AUTHENTIFICATION_ROOT;
import static org.waarp.openr66.configuration.RuleFileBasedConfiguration.*;
import static org.waarp.openr66.database.data.DbHostConfiguration.*;

/** An interface for POJOs used in XML (de)serialization. */
@SuppressWarnings("CanBeFinal")
public interface XmlSerializable {

    /** A POJO representing a list of business ids for XML (de)serialisation purposes. */
    @XmlRootElement(name = XML_BUSINESS)
    class Businesses implements XmlSerializable {
        /** The list of host names. */
        @XmlElement(name = XML_BUSINESSID)
        public List<String> business = new ArrayList<String>();
    }

    /** A POJO representing a list of roles for XML (de)serialisation purposes. */
    @XmlRootElement(name = XML_ROLES)
    class Roles implements XmlSerializable {
        /** The list of {@link RoleEntry}. */
        @XmlElement(name = XML_ROLE)
        public List<RoleEntry> roles = new ArrayList<RoleEntry>();

        public Roles() {}

        public Roles(List<RoleEntry> roles) {
            this.roles = roles;
        }

        /** A POJO representing a role entry for XML (de)serialisation purposes. */
        @XmlType
        public static class RoleEntry {
            /** The host's id. */
            @XmlElement(name = XML_ROLEID)
            public String hostName;

            /** The list of allowed actions on the server. */
            @XmlElement(name = XML_ROLESET)
            @XmlList
            public List<RoleDefault.ROLE> roleList = new ArrayList<RoleDefault.ROLE>();
        }
    }

    /** A POJO representing a list of aliases for XML (de)serialisation purposes. */
    @XmlRootElement(name = XML_ALIASES)
    class Aliases implements XmlSerializable {
        /** The list of {@link AliasEntry}. */
        @XmlElement(name = XML_ALIAS)
        public List<AliasEntry> aliases = new ArrayList<AliasEntry>();

        public Aliases() {}

        public Aliases(List<AliasEntry> aliases) {
            this.aliases = aliases;
        }

        /** A POJO representing an alias entry for XML (de)serialisation purposes. */
        @XmlType
        public static class AliasEntry {
            /** The host's id. */
            @XmlElement(name = XML_REALID)
            public String hostName;

            /** The list of the server's known aliases. */
            @XmlElement(name = XML_ALIASID)
            @XmlList
            public List<String> aliasList = new ArrayList<String>();
        }
    }

    @XmlRootElement(name = XML_AUTHENTIFICATION_ROOT)
    class Hosts implements XmlSerializable {
        @XmlElement(name = XML_AUTHENTIFICATION_ENTRY)
        public List<Host> hosts = new ArrayList<Host>();

        public Hosts() {}

        public Hosts(List<Host> hosts) {
            this.hosts = hosts;
        }
    }

    @XmlRootElement(name = MULTIPLEROOT)
    class Rules implements XmlSerializable {
        @XmlElement(name = RuleFileBasedConfiguration.ROOT)
        public List<Rule> rules = new ArrayList<Rule>();

        public Rules() {}

        public Rules(List<Rule> rules) {
            this.rules = rules;
        }

        @XmlType(name = XTASKS)
        public static class Tasks {
            @XmlElementWrapper(name = XTASKS)
            @XmlElement(name = XTASK)
            public List<RuleTask> tasks;

            public Tasks() {}

            public Tasks(List<RuleTask> tasks) {
                this.tasks = tasks;
            }
        }
    }

    @XmlRootElement(name = DbTaskRunner.XMLRUNNERS)
    class Transfers implements XmlSerializable {
        @XmlElement(name = DbTaskRunner.XMLRUNNER)
        public List<Transfer> transfers = new ArrayList<Transfer>();

        public Transfers() {}

        public Transfers(List<Transfer> transfers) {
            this.transfers = transfers;
        }
    }
}
