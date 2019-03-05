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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.waarp.common.database.DbConstant;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.protocol.http.restv2.RestUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.waarp.openr66.protocol.http.restv2.RestUtils.HOST_ID;


/** RestHost configuration JSON object for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class RestHostConfig {

    public RestHostConfig() {}

    public RestHostConfig(Business business) {
        this.business = (business.getBusiness().isEmpty()) ? new String[0] : business.getBusiness().split(" ");
        this.others = business.getOthers().replaceAll("</?root>|</?version>", "");

        this.roles = Role.toRoleList(business.getRoles());

        if(business.getAliases().matches("<aliases></aliases>")) {
            this.aliases = new Alias[0];
        } else {
            String aliasesStr = business.getAliases().replaceAll("<aliases><alias>|</alias></aliases>", "");
            String[] aliases = aliasesStr.split("</alias><alias>");
            this.aliases = new Alias[aliases.length];
            for (int i = 0; i < aliases.length; i++) {
                this.aliases[i] = new Alias(aliases[i]);
            }
        }
    }

    /** A pair associating a host with the type of actions it is allowed to perform on the server. */
    @ConsistencyCheck
    public static class Role {
        /** The host's id. */
        @NotEmpty
        public String host;

        /** The list of allowed actions on the server. */
        public RoleDefault.ROLE[] roleTypes;

        public Role(){}

        /**
         * Constructs a new role from a host and a list of actions.
         *
         * @param host      The host's id.
         * @param roleTypes The host's allowed actions.
         */
        public Role(String host, RoleDefault.ROLE[] roleTypes) {
            this.host = host;
            this.roleTypes = roleTypes;
        }

        /**
         * Constructs a new role entry from a database xml role string.
         * @param business  The role xml string.
         */
        public Role(String business) {
            Pattern pattern = Pattern.compile("(<roleid>)(.+)(</roleid><roleset>)(.+)(</roleset>)");
            Matcher matcher = pattern.matcher(business);
            if(matcher.find()) {
                this.host = matcher.group(2);

                String[] roles = matcher.group(4).split(" ");
                this.roleTypes = new RoleDefault.ROLE[roles.length];
                for(int i=0; i<roles.length; i++) {
                    this.roleTypes[i] = RoleDefault.ROLE.valueOf(roles[i]);
                }
            } else {
                throw new IllegalArgumentException("Could not match the role pattern.");
            }
        }

        public static Role[] toRoleList(String business) {
            if(business.matches("<roles></roles>")) {
                return new Role[0];
            } else {
                String rolesStr = business.replaceAll("<roles><role>|</role></roles>", "");
                String[] roles = rolesStr.split("</role><role>");
                Role[] restRoles = new Role[roles.length];
                for (int i = 0; i < roles.length; i++) {
                    restRoles[i] = new Role(roles[i]);
                }
                return restRoles;
            }
        }

        public String toBusiness() {
            StringBuilder business = new StringBuilder("<role><roleid>" + this.host + "</roleid><roleset>");
            for(RoleDefault.ROLE roleType : this.roleTypes) {
                business.append(roleType.name());
            }
            business = new StringBuilder(business.toString().trim());
            business.append("</roleset></role>");
            return business.toString();
        }
    }

    /** A pair associating a host with it's known aliases. */
    @ConsistencyCheck
    public static class Alias {
        /** The host's id. */
        @NotEmpty
        public String host;

        /** The list of the server's known aliases. */
        @NotEmpty
        public String[] aliasSet;

        public Alias(){}

        /**
         * Constructs a new alias entry from a host and a list of alias.
         *
         * @param host      The host's id.
         * @param aliasSet  The host's known aliases.
         */
        public Alias(String host, String[] aliasSet) {
            this.host = host;
            this.aliasSet = aliasSet;
        }

        /**
         * Constructs a new alias entry from an xml alias string typically found in the database.
         * @param business  The alias xml string.
         */
        public Alias(String business) {
            Pattern pattern = Pattern.compile("(<realid>)(.+)(</realid><aliasid>)(.+)(</aliasid>)");
            Matcher matcher = pattern.matcher(business);
            if(matcher.find()) {
                this.host = matcher.group(2);
                this.aliasSet = matcher.group(4).split(" ");
            } else {
                throw new IllegalArgumentException("Could not match the alias pattern.");
            }
        }

        public String toBusiness() {
            return "<alias><realid>" + this.host + "</realid><aliasid>" + RestUtils.toArrayDbList(this.aliasSet) +
                    "</aliasid></alias>";
        }
    }

    /** The host of the host using this configuration. */
    @JsonIgnore
    public final String hostID = HOST_ID;

    /** The list of al hosts allowed to make request to execute the server's business. */
    public String[] business = new String[0];

    /**
     * The list of all hosts paired with the list of actions they are each allowed to perform on the server.
     *
     * @see Role
     */
    public Role[] roles = new Role[0];

    /**
     * The list of all hosts paired with the list of their known aliases.
     *
     * @see Alias
     */
    public Alias[] aliases = new Alias[0];

    /** The database configuration version in XML format. */
    public String others = "";


    public Business toBusiness() {
        String business = RestUtils.toArrayDbList(this.business);

        StringBuilder aliases = new StringBuilder("<aliases>");
        for(Alias alias : this.aliases) {
            aliases.append(alias.toBusiness());
        }
        aliases.append("</aliases>");
        StringBuilder roles = new StringBuilder("<roles>");
        for(Role role : this.roles) {
            roles.append(role.toBusiness());
        }
        roles.append("</roles>");

        String others = "<root><version>" + this.others + "</version></root>";

        return new Business(this.hostID, business, roles.toString(), aliases.toString(), others);
    }
}
