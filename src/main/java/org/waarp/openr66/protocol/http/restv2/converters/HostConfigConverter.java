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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Aliases;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Aliases.AliasEntry;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Businesses;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Roles;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Roles.RoleEntry;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.utils.XmlUtils;

import javax.ws.rs.InternalServerErrorException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.waarp.common.role.RoleDefault.ROLE;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.SERVER_NAME;
import static org.waarp.openr66.protocol.http.restv2.converters.HostConfigConverter.FieldNames.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_FIELD_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.MISSING_FIELD;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.UNKNOWN_FIELD;


/**
 * A collection of utility methods to convert {@link Business} POJOs
 * to {@link ObjectNode} and vice-versa.
 */
public final class HostConfigConverter {

    @SuppressWarnings("unused")
    public static final class FieldNames {
        public static final String BUSINESS = "business";
        public static final String ROLES = "roles";
        public static final String ALIASES = "aliases";
        public static final String OTHERS = "others";
        public static final String HOST_NAME = "hostName";
        public static final String ROLE_LIST = "roleList";
        public static final String ALIAS_LIST = "aliasList";
    }

    public static ObjectNode businessToNode(Business hostConfig) {
        ArrayNode business = getBusinessArray(hostConfig);
        ArrayNode roles = getRolesArray(hostConfig);
        ArrayNode aliasArray = getAliasArray(hostConfig);

        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.putArray(BUSINESS).addAll(business);
        node.putArray(ROLES).addAll(roles);
        node.putArray(ALIASES).addAll(aliasArray);
        node.put(OTHERS, hostConfig.getOthers());

        return node;
    }

    private static ArrayNode getAliasArray(Business hostConfig) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);

        Aliases aliases = XmlUtils.xmlToObject(hostConfig.getAliases(), Aliases.class);

        for (AliasEntry alias : aliases.aliases) {
            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
            ArrayNode aliasIds = new ArrayNode(JsonNodeFactory.instance);

            for (String aliasId : alias.aliasList) {
                aliasIds.add(aliasId);
            }

            node.put(HOST_NAME, alias.hostName);
            node.putArray(ALIAS_LIST).addAll(aliasIds);

            array.add(node);
        }

        return array;
    }

    private static ArrayNode getRolesArray(Business hostConfig) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);

        Roles roles = XmlUtils.xmlToObject(hostConfig.getRoles(), Roles.class);

        for (RoleEntry role : roles.roles) {
            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
            ArrayNode roleTypes = new ArrayNode(JsonNodeFactory.instance);

            for (ROLE roleType : role.roleList) {
                roleTypes.add(roleType.name());
            }

            node.put(HOST_NAME, role.hostName);
            node.putArray(ROLE_LIST).addAll(roleTypes);

            array.add(node);
        }

        return array;
    }

    private static ArrayNode getBusinessArray(Business hostConfig) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
        Businesses business = XmlUtils.xmlToObject(hostConfig.getBusiness(), Businesses.class);

        for (String businessId : business.business) {
            array.add(businessId);
        }
        return array;
    }

    private static Aliases nodeToAliasList(ArrayNode array)
            throws UserErrorException {
        List<AliasEntry> aliases = new ArrayList<AliasEntry>();
        List<Error> errors = new ArrayList<Error>();

        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            AliasEntry alias = new AliasEntry();

            if (!element.isObject()) {
                errors.add(ILLEGAL_PARAMETER_VALUE(ALIASES, element.toString()));
                continue;
            }

            Iterator<Map.Entry<String, JsonNode>> fields = element.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String name = field.getKey();
                JsonNode value = field.getValue();

                if (name.equalsIgnoreCase(HOST_NAME)) {
                    if (value.isTextual()) {
                        alias.hostName = value.asText();
                    } else {
                        errors.add(ILLEGAL_PARAMETER_VALUE(HOST_NAME, value.toString()));
                    }
                }
                else if (name.equalsIgnoreCase(ALIAS_LIST)) {
                    if (value.isArray()) {
                        Iterator<JsonNode> aliasList = value.elements();
                        while (aliasList.hasNext()) {
                            JsonNode aliasId = aliasList.next();
                            if (aliasId.isTextual()) {
                                alias.aliasList.add(aliasId.asText());
                            } else {
                                errors.add(ILLEGAL_PARAMETER_VALUE(ALIAS_LIST,
                                        aliasId.toString()));
                            }
                        }
                    } else {
                        errors.add(ILLEGAL_PARAMETER_VALUE(ALIAS_LIST, value.toString()));
                    }
                }
                else {
                    errors.add(UNKNOWN_FIELD(name));
                }
            }

            if (alias.hostName.isEmpty()) {
                errors.add(MISSING_FIELD(HOST_NAME));
            }
            if (alias.aliasList.isEmpty()) {
                errors.add(MISSING_FIELD(ALIAS_LIST));
            }
            aliases.add(alias);
        }

        if (errors.isEmpty()) {
            return new Aliases(aliases);
        } else {
            throw new UserErrorException(errors);
        }
    }

    private static Roles nodeToRoles(ArrayNode array)
            throws UserErrorException {
        List<RoleEntry> roles = new ArrayList<RoleEntry>();
        List<Error> errors = new ArrayList<Error>();

        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            RoleEntry role = new RoleEntry();

            if (!element.isObject()) {
                errors.add(ILLEGAL_PARAMETER_VALUE(ROLES, element.toString()));
                continue;
            }

            Iterator<Map.Entry<String, JsonNode>> fields = element.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String name = field.getKey();
                JsonNode value = field.getValue();

                if (name.equalsIgnoreCase(HOST_NAME)) {
                    if (value.isTextual()) {
                        role.hostName = value.asText();
                    } else {
                        errors.add(ILLEGAL_PARAMETER_VALUE(HOST_NAME, value.toString()));
                    }
                }
                else if (name.equalsIgnoreCase(ROLE_LIST)) {
                    if (value.isArray()) {
                        Iterator<JsonNode> roleTypes = value.elements();
                        while (roleTypes.hasNext()) {
                            JsonNode roleType = roleTypes.next();
                            if (roleType.isTextual()) {
                                try {
                                    role.roleList.add(ROLE.valueOf(roleType.asText()));
                                } catch (IllegalArgumentException e) {
                                    errors.add(ILLEGAL_PARAMETER_VALUE(ROLE_LIST,
                                            roleType.toString()));
                                }
                            } else {
                                errors.add(ILLEGAL_PARAMETER_VALUE(ROLE_LIST,
                                        roleType.toString()));
                            }
                        }
                    } else {
                        errors.add(ILLEGAL_PARAMETER_VALUE(ROLE_LIST, value.toString()));
                    }
                }
                else {
                    errors.add(UNKNOWN_FIELD(name));
                }
            }

            if (role.hostName.isEmpty()) {
                errors.add(MISSING_FIELD(HOST_NAME));
            }
            if (role.roleList.isEmpty()) {
                errors.add(MISSING_FIELD(ROLE_LIST));
            }
            roles.add(role);
        }

        if (errors.isEmpty()) {
            return new Roles(roles);
        } else {
            throw new UserErrorException(errors);
        }
    }

    public static Business nodeToNewBusiness(ObjectNode object)
            throws UserErrorException {
        Business defaultBusiness = new Business(SERVER_NAME, "", "<roles></roles>",
                "<aliases></aliases>", "<root><version></version></root>");

        return parseNode(object, defaultBusiness);
    }

    public static Business nodeToUpdatedBusiness(ObjectNode object, Business oldBusiness) {
        return parseNode(object, oldBusiness);
    }

    public static List<ROLE> getRoles(String hostName) {
        ArrayNode array;
        BusinessDAO businessDAO = null;
        try {
            businessDAO = DAO_FACTORY.getBusinessDAO();
            Business config = businessDAO.select(SERVER_NAME);
            array = getRolesArray(config);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (businessDAO != null) {
                businessDAO.close();
            }
        }

        Roles roles = nodeToRoles(array);

        for (RoleEntry role : roles.roles) {
            if (role.hostName.equals(hostName)) {
                return role.roleList;
            }
        }
        return null;
    }

    private static Business parseNode(ObjectNode object, Business hostConfig)
            throws UserErrorException {
        List<Error> errors = new ArrayList<Error>();

        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();

            if (name.equalsIgnoreCase(BUSINESS)) {
                if (value.isArray()) {
                    Businesses businessList = new Businesses();
                    Iterator<JsonNode> business = value.elements();
                    while (business.hasNext()) {
                        JsonNode businessName = business.next();
                        if (businessName.isTextual()) {
                            businessList.business.add(businessName.asText());
                        } else {
                            errors.add(ILLEGAL_FIELD_VALUE(BUSINESS,
                                    businessName.toString()));
                        }
                    }
                    hostConfig.setBusiness(XmlUtils.objectToXml(businessList));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(BUSINESS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(ROLES)) {
                if (value.isArray()) {
                    try {
                        Roles roles = nodeToRoles((ArrayNode) value);
                        hostConfig.setRoles(XmlUtils.objectToXml(roles));
                    } catch (UserErrorException e) {
                        errors.addAll(e.errors);
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ROLES, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(ALIASES)) {
                if (value.isArray()) {
                    try {
                        Aliases aliases = nodeToAliasList((ArrayNode) value);
                        hostConfig.setAliases(XmlUtils.objectToXml(aliases));
                    } catch (UserErrorException e) {
                        errors.addAll(e.errors);
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ALIASES, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(OTHERS)) {
                if (value.isTextual() && value.asText()
                        .matches("<root><version>.+</version></root>")) {
                    hostConfig.setOthers(value.asText());

                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ALIASES, value.toString()));
                }
            }
            else {
                errors.add(UNKNOWN_FIELD(name));
            }
        }

        if (errors.isEmpty()) {
            return hostConfig;
        } else {
            throw new UserErrorException(errors);
        }
    }

    /** Prevents the default constructor from being called. */
    private HostConfigConverter() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }
}
