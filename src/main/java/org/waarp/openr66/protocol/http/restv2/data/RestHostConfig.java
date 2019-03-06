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

import org.waarp.openr66.pojo.Business;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.InternalServerErrorException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlList;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;

import static org.waarp.common.role.RoleDefault.ROLE;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.arrayToSpaceString;


/** RestHost configuration JSON object for Rest HTTP support for R66. */
@SuppressWarnings({"unused"})
public class RestHostConfig {

    public RestHostConfig() {}

    public RestHostConfig(Business business) {
        try {
            this.business = (business.getBusiness().isEmpty()) ?
                    new String[0] : business.getBusiness().split(" ");

            this.others = business.getOthers();
            this.roles = Role.toRoleArray(business.getRoles());
            this.aliases = Alias.toAliasList(business.getAliases());
        } catch (JAXBException e) {
            throw new InternalServerErrorException(e);
        }
    }

    /**
     * A pair associating a host with the type of actions it is allowed to
     * perform on the server.
     */
    @ConsistencyCheck
    @XmlType(name = "restrole")
    public static class Role {
        /** The host's id. */
        @Required
        @XmlElement(name = "roleid")
        public String hostName;

        /** The list of allowed actions on the server. */
        @XmlElement(name = "roleset")
        @XmlList
        public ROLE[] roleTypes = new ROLE[0];

        @XmlRootElement(name = "roles")
        private static class RestRoleList {
            @XmlElement(name = "role")
            public Role[] roles;
        }

        public static Role[] toRoleArray(String roles_str) throws JAXBException {
            StreamSource businessSource = new StreamSource(new StringReader(roles_str));
            JAXBContext context = JAXBContext.newInstance(RestRoleList.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            Role[] roles = unmarshaller.unmarshal(businessSource, RestRoleList.class)
                    .getValue().roles;

            return (roles == null) ? new Role[0] : roles;
        }


        public static String toDbRoles(Role[] roles) throws JAXBException {
            RestRoleList restRoles = new RestRoleList();
            restRoles.roles = roles;

            StringWriter writer = new StringWriter();
            JAXBContext context = JAXBContext.newInstance(RestRoleList.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.marshal(restRoles, writer);

            return writer.toString();
        }
    }

    /** A pair associating a host with it's known aliases. */
    @ConsistencyCheck
    public static class Alias {
        /** The host's id. */
        @Required
        @XmlElement(name = "realid")
        public String host;

        /** The list of the server's known aliases. */
        @XmlElement(name = "aliasid")
        public String[] aliasSet = new String[0];

        @XmlRootElement(name = "aliases")
        private static class RestAliasList {
            @XmlElement(name = "alias")
            public Alias[] aliases;
        }

        public static Alias[] toAliasList(String alias_str) throws JAXBException {
            StreamSource businessSource = new StreamSource(new StringReader(alias_str));
            JAXBContext context = JAXBContext.newInstance(RestAliasList.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            Alias[] aliases = unmarshaller.unmarshal(businessSource, RestAliasList.class)
                    .getValue().aliases;

            return (aliases == null) ? new Alias[0] : aliases;
        }

        public static String toDbAliases(Alias[] aliases) throws JAXBException {
            RestAliasList restAliases = new RestAliasList();
            restAliases.aliases = aliases;

            StringWriter writer = new StringWriter();
            JAXBContext context = JAXBContext.newInstance(RestAliasList.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.marshal(restAliases, writer);

            return writer.toString();
        }
    }

    /** The list of al hosts allowed to execute the server's business. */
    public String[] business;

    /**
     * The list of all hosts paired with the list of actions they are each
     * allowed to perform on the server.
     *
     * @see Role
     */
    public Role[] roles;

    /**
     * The list of all hosts paired with the list of their known aliases.
     *
     * @see Alias
     */
    public Alias[] aliases;

    /** The database configuration version in XML format. */
    @DefaultValue("")
    public String others;


    public Business toBusiness() {
        try {
            String business = arrayToSpaceString(this.business);

            String aliases = Alias.toDbAliases(this.aliases);
            String roles = Role.toDbRoles(this.roles);

            String others = this.others;

            return new Business(HOST_ID, business, roles, aliases, others);
        } catch (JAXBException e) {
            throw new InternalServerErrorException(e);
        }
    }
}
