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
import org.waarp.openr66.dao.exception.DAOConnectionException;
import org.waarp.openr66.dao.exception.DAONoDataException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.protocol.http.restv2.errors.RestError;
import org.waarp.openr66.protocol.http.restv2.errors.RestErrorException;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Aliases;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Aliases.AliasEntry;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Businesses;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Roles;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Roles.RoleEntry;
import org.waarp.openr66.protocol.http.restv2.utils.XmlUtils;

import javax.ws.rs.InternalServerErrorException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.waarp.common.role.RoleDefault.ROLE;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.SERVER_NAME;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HostConfigFields.*;
import static org.waarp.openr66.protocol.http.restv2.errors.RestErrors.*;


/**
 * A collection of utility methods to convert {@link Business}
 * objects to their corresponding {@link ObjectNode} and vice-versa.
 */
public final class HostConfigConverter {

    /** Makes the default constructor of this utility class inaccessible. */
    private HostConfigConverter() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    //########################### PUBLIC METHODS ###############################

    /**
     * Converts the given {@link Business} object into an {@link ObjectNode}.
     *
     * @param hostConfig the HostConfig object to convert
     * @return           the converted ObjectNode
     */
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

    /**
     * Converts the given {@link ObjectNode} into a {@link Business} object.
     *
     * @param object the ObjectNode to convert
     * @return       the corresponding HostConfig object
     * @throws RestErrorException if the given ObjectNode does not represent a
     *                            Business object
     */
    public static Business nodeToNewBusiness(ObjectNode object) {
        Business emptyBusiness = new Business(SERVER_NAME, "", "<roles></roles>",
                "<aliases></aliases>", "<root><version></version></root>");

        return nodeToUpdatedBusiness(object, emptyBusiness);
    }

    /**
     * Returns the given {@link Business} object updated with the values
     * defined in the {@link ObjectNode} parameter. All fields missing from
     * the ObjectNode parameter will stay unchanged in the returned Business
     * object.
     *
     * @param object      the ObjectNode to convert.
     * @param oldBusiness the Business object to update.
     * @return            the updated Business object
     * @throws RestErrorException if the given ObjectNode does not represent a
     *                            Business object
     */
    public static Business nodeToUpdatedBusiness(ObjectNode object, Business oldBusiness) {
        List<RestError> errors = new ArrayList<RestError>();

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
                    oldBusiness.setBusiness(XmlUtils.objectToXml(businessList));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(BUSINESS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(ROLES)) {
                if (value.isArray()) {
                    try {
                        Roles roles = nodeToRoles((ArrayNode) value);
                        oldBusiness.setRoles(XmlUtils.objectToXml(roles));
                    } catch (RestErrorException e) {
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
                        oldBusiness.setAliases(XmlUtils.objectToXml(aliases));
                    } catch (RestErrorException e) {
                        errors.addAll(e.errors);
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ALIASES, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(OTHERS)) {
                if (value.isTextual() && value.asText()
                        .matches("<root><version>.+</version></root>")) {
                    oldBusiness.setOthers(value.asText());

                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ALIASES, value.toString()));
                }
            }
            else {
                errors.add(UNKNOWN_FIELD(name));
            }
        }

        if (errors.isEmpty()) {
            return oldBusiness;
        } else {
            throw new RestErrorException(errors);
        }
    }


    /**
     * Returns the requested host's list of roles on the server.
     * 
     * @param hostName the desired host's name
     * @return         the host's list of roles
     * @throws InternalServerErrorException if an unexpected error occurred
     */
    public static List<ROLE> getRoles(String hostName) {
        ArrayNode array;
        BusinessDAO businessDAO = null;
        try {
            businessDAO = DAO_FACTORY.getBusinessDAO();
            Business config = businessDAO.select(SERVER_NAME);
            array = getRolesArray(config);
        } catch (DAOConnectionException e) {
            throw new InternalServerErrorException(e);
        } catch (DAONoDataException e) {
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


    //########################## PRIVATE METHODS ###############################

    /**
     * Converts and returns the list of aliases of the given {@link Business}
     * object into an {@link ArrayNode}.
     *
     * @param hostConfig the server's HostConfig object
     * @return           the server's list of known aliases
     */
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

    /**
     * Converts and returns the list of roles of the given {@link Business}
     * object into an {@link ArrayNode}.
     *
     * @param hostConfig the server's HostConfig object
     * @return           the server's list of known aliases
     */
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

    /**
     * Converts and returns the list of business partners of the given
     * {@link Business} object into an {@link ArrayNode}.
     *
     * @param hostConfig the server's HostConfig object
     * @return           the server's list of known aliases
     */
    private static ArrayNode getBusinessArray(Business hostConfig) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
        Businesses business = XmlUtils.xmlToObject(hostConfig.getBusiness(), Businesses.class);

        for (String businessId : business.business) {
            array.add(businessId);
        }
        return array;
    }

    /**
     * Converts the given {@link ArrayNode} into an {@link Aliases} object.
     *
     * @param array the JSON array of aliases
     * @return      the XML serializable list of aliases
     * @throws RestErrorException if the given ArrayNode does not represent a
     *                            list of aliases
     */
    private static Aliases nodeToAliasList(ArrayNode array) {
        List<AliasEntry> aliases = new ArrayList<AliasEntry>();
        List<RestError> errors = new ArrayList<RestError>();

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
            throw new RestErrorException(errors);
        }
    }

    /**
     * Converts the given {@link ArrayNode} into an {@link Roles} object.
     *
     * @param array the JSON list of roles
     * @return      the XML serializable list of roles
     * @throws RestErrorException if the given ArrayNode does not represent
     *                            a list of roles
     */
    private static Roles nodeToRoles(ArrayNode array) {
        List<RoleEntry> roles = new ArrayList<RoleEntry>();
        List<RestError> errors = new ArrayList<RestError>();

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
            throw new RestErrorException(errors);
        }
    }
}
