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

package org.waarp.openr66.protocol.http.restv2.dbhandlers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.pojo.UpdatedInfo;
import org.waarp.openr66.protocol.http.restv2.converters.ServerStatusMaker;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Hosts;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Rules;
import org.waarp.openr66.protocol.http.restv2.utils.XmlSerializable.Transfers;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.errors.Errors;
import org.waarp.openr66.protocol.http.restv2.utils.JsonUtils;
import org.waarp.openr66.protocol.http.restv2.utils.RestUtils;
import org.waarp.openr66.protocol.http.restv2.utils.XmlUtils;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.lang.Boolean.TRUE;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.gateway.kernel.rest.RestConfiguration.CRUD;
import static org.waarp.openr66.dao.database.DBTransferDAO.*;
import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.*;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.*;
import static org.waarp.openr66.protocol.http.restv2.dbhandlers.ServerHandler.EntryPoints.*;
import static org.waarp.openr66.protocol.http.restv2.dbhandlers.ServerHandler.Params.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;

/**
 * This is the {@link AbstractRestDbHandler} handling all system commands
 * sent to the host.
 */

@Path(SERVER_HANDLER_URI)
public class ServerHandler extends AbstractRestDbHandler {

    /**
     *  Stores the names of all the sub directories of the {@link ServerHandler}
     *  handler as defined in the REST API specification.
     */
    @SuppressWarnings("unused")
    public static final class EntryPoints {
        public static final String STATUS_URI = "status";
        public static final String DEACTIVATE_URI = "deactivate";
        public static final String SHUTDOWN_URI = "shutdown";
        public static final String RESTART_URI = "restart";
        public static final String LOGS_URI = "logs";
        public static final String CONFIG_URI = "config";
        public static final String BUSINESS_URI = "business";
    }

    /**
     * Stores the names of all query parameters for all entry points handled
     * by this {@link ServerHandler} as defined in the REST API specification.
     */
    @SuppressWarnings("unused")
    public static final class Params {
        //Server status
        public static final String PERIOD = "period";

        //Log export
        public static final String PURGE = "purge";
        public static final String CLEAN = "clean";
        public static final String STATUS = "status";
        public static final String RULE_NAME = "ruleName";
        public static final String START = "start";
        public static final String STOP = "stop";
        public static final String START_ID = "startID";
        public static final String STOP_ID = "stopID";
        public static final String REQUESTED = "requester";

        //Config export
        public static final String EXPORT_HOSTS = "exportHosts";
        public static final String EXPORT_RULES = "exportRules";
        public static final String EXPORT_BUSINESS = "exportBusiness";
        public static final String EXPORT_ALIASES = "exportAliases";
        public static final String EXPORT_ROLES = "exportRoles";

        //Config import
        public static final String PURGE_HOST = "purgeHosts";
        public static final String PURGE_RULE = "purgeRules";
        public static final String PURGE_BUSINESS = "purgeBusiness";
        public static final String PURGE_ALIASES = "purgeAliases";
        public static final String PURGE_ROLES = "purgeRoles";
        public static final String HOST_FILE = "hostsFile";
        public static final String RULE_FILE = "rulesFile";
        public static final String BUSINESS_FILE = "businessFile";
        public static final String ALIAS_FILE = "aliasesFile";
        public static final String ROLE_FILE = "rolesFile";
    }

    /** Stores the path to the archive directory. */
    private static final String ARCH_PATH =
            configuration.getBaseDirectory() +
            configuration.getArchivePath();

    /** Stores the path to the configuration directory. */
    private static final String CONFIGS_PATH =
            configuration.getBaseDirectory() +
            configuration.getConfigPath();

    /**
     * Stores a {@link Map} associating each sub-path of the handler to their
     * respective CRUD configuration.
     */
    private final Map<String, Byte> serverCRUD = new HashMap<String, Byte>();

    /**
     * Instantiates the handler with the given CRUD configuration array.
     *
     * @param crud An array of byte containing all the REST CRUD configurations.
     */
    public ServerHandler(byte[] crud) {
        super((byte) 0);
        serverCRUD.put(STATUS_URI, crud[Information.ordinal()]);
        serverCRUD.put(DEACTIVATE_URI, crud[Server.ordinal()]);
        serverCRUD.put(SHUTDOWN_URI, crud[Server.ordinal()]);
        serverCRUD.put(RESTART_URI, crud[Server.ordinal()]);
        serverCRUD.put(LOGS_URI, crud[Log.ordinal()]);
        serverCRUD.put(CONFIG_URI, crud[Config.ordinal()]);
        serverCRUD.put(BUSINESS_URI, crud[Business.ordinal()]);
    }

    /**
     * Checks if the request can be made in consideration to the handler's CRUD
     * configuration.
     *
     * @param request  The {@link HttpRequest} made to the handler.
     * @return  Returns {@code true} if the request is valid, {@code false}
     *          if the CRUD configuration does not allow this request.
     */
    @Override
    public boolean checkCRUD(HttpRequest request) {
        Pattern pattern = Pattern.compile(
                "(" + SERVER_HANDLER_URI + ")([\\w/]+)(\\?.+)?");
        Matcher matcher = pattern.matcher(request.uri());
        HttpMethod method = request.method();

        if (!matcher.find()) { return false; }
        String subPath = matcher.group(2);
        Byte crud = serverCRUD.get(subPath);
        if (crud == null) { return false; }
        if (method.equals(GET)) {
            return CRUD.READ.isValid(crud);
        }
        else if (method.equals(POST)) {
            return CRUD.CREATE.isValid(crud);
        }
        else if (method.equals(DELETE)) {
            return CRUD.DELETE.isValid(crud);
        }
        else if (method.equals(PUT)) {
            return CRUD.UPDATE.isValid(crud);
        }
        else return method.equals(OPTIONS);
    }


    /**
     * Get the general status of the server.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(STATUS_URI)
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(READONLY)
    public void getStatus(HttpRequest request, HttpResponder responder,
                          @QueryParam(PERIOD) @DefaultValue("P1DT0H0M0S")
                                  String period_str) {
        try {
            long seconds = Period.parse(period_str).toStandardSeconds().getSeconds();
            ObjectNode status = ServerStatusMaker.exportAsJson(seconds);
            String responseText = JsonUtils.nodeToString(status);
            responder.sendJson(OK, responseText);
        } catch (IllegalArgumentException e) {
            throw new UserErrorException(Errors.ILLEGAL_PARAMETER_VALUE(PERIOD, period_str));
        } catch (UnsupportedOperationException e) {
            throw new UserErrorException(Errors.ILLEGAL_PARAMETER_VALUE(PERIOD, period_str));
        }
    }

    /**
     * Deactivates the server so that it doesn't accept any new transfer request.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(DEACTIVATE_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(FULLADMIN)
    public void deactivate(HttpRequest request, HttpResponder responder) {
        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            Host host = hostDAO.select(SERVER_NAME);
            host.setActive(!host.isActive());
            hostDAO.update(host);

            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add("active", host.isActive());
            responder.sendStatus(NO_CONTENT, headers);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }
    }

    /**
     * Shut down the server.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(SHUTDOWN_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(FULLADMIN)
    public void shutdown(HttpRequest request, HttpResponder responder) {
        R66ShutdownHook.setRestart(false);
        ChannelUtils.startShutdown();
        responder.sendStatus(NO_CONTENT);
    }

    /**
     * Restart the server.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(RESTART_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(FULLADMIN)
    public void restart(HttpRequest request, HttpResponder responder) {
        R66ShutdownHook.setRestart(true);
        ChannelUtils.startShutdown();
        responder.sendStatus(NO_CONTENT);
    }


    /**
     * Export the server logs to a file. Only the entries that satisfy
     * the desired filters will be exported.
     *
     * @param request    The {@link HttpRequest} made on the resource.
     * @param responder  The {@link HttpResponder} which sends the reply to
     *                   the request.
     * @param purge_str  HTTP query parameter, states whether to delete exported
     *                   entries or not.
     * @param clean_str  HTTP query parameter, states whether to fix the
     *                   incoherent entries.
     * @param status_str HTTP query parameter, only transfer in one of these
     *                   statuses will be exported.
     * @param rule       HTTP query parameter, only transfer using this rule
     *                   will be exported.
     * @param start      HTTP query parameter, lower bound for the date of the transfer.
     * @param stop       HTTP query parameter, upper bound for the date of the transfer.
     * @param startID    HTTP query parameter, lower bound for the transfer's ID.
     * @param stopID     HTTP query parameter, upper bound for the transfer's ID.
     */
    @Path(LOGS_URI)
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(LOGCONTROL)
    public void getLogs(HttpRequest request, HttpResponder responder,
                        @QueryParam(PURGE) @DefaultValue("false") String purge_str,
                        @QueryParam(CLEAN) @DefaultValue("false") String clean_str,
                        @QueryParam(STATUS) @DefaultValue("") String status_str,
                        @QueryParam(RULE_NAME) @DefaultValue("") String rule,
                        @QueryParam(START) @DefaultValue("") String start,
                        @QueryParam(STOP) @DefaultValue("") String stop,
                        @QueryParam(START_ID) @DefaultValue("") String startID,
                        @QueryParam(STOP_ID) @DefaultValue("") String stopID,
                        @QueryParam(REQUESTED) @DefaultValue("") String requester) {

        List<Error> errors = new ArrayList<Error>();
        Locale lang = RestUtils.getRequestLocale(request);
        List<Filter> filters = new ArrayList<Filter>();
        String filePath = ARCH_PATH + File.separator + SERVER_NAME +
                "_export_" + DateTime.now().toString() + ".xml";


        Boolean purge = false, clean = false;
        try {
            purge = RestUtils.stringToBoolean(purge_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(PURGE, purge_str));
        }
        try {
            clean = RestUtils.stringToBoolean(clean_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(CLEAN, clean_str));
        }

        if (!start.isEmpty()) {
            try {
                DateTime lowerDate = DateTime.parse(start);
                filters.add(new Filter(TRANSFER_START_FIELD, ">=", lowerDate));
            } catch(IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(START, start));
            }
        }
        if (!stop.isEmpty()) {
            try {
                DateTime upperDate = DateTime.parse(stop);
                filters.add(new Filter(TRANSFER_START_FIELD, "<=", upperDate));
            } catch(IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(STOP, stop));
            }
        }
        if (!startID.isEmpty()) {
            try {
                Long lowerID = Long.parseLong(startID);
                filters.add(new Filter(ID_FIELD, ">=", lowerID));
            } catch (NumberFormatException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(START_ID, startID));
            }
        }
        if (!stopID.isEmpty()) {
            try {
                Long upperID = Long.parseLong(stopID);
                filters.add(new Filter(ID_FIELD, "<=", upperID));
            } catch (NumberFormatException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(STOP_ID, stopID));
            }
        }
        if (!rule.isEmpty()) {
            filters.add(new Filter(ID_RULE_FIELD, "=", rule));
        }
        if (!status_str.isEmpty()) {
            try {
                UpdatedInfo status = UpdatedInfo.valueOf(status_str);
                filters.add(new Filter(UPDATED_INFO_FIELD, "=", status.ordinal()));
            } catch (IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(STATUS, status_str));
            }
        }

        if (!requester.isEmpty()) {
            filters.add(new Filter(REQUESTER_FIELD, "=", requester));
        }

        if (!errors.isEmpty()) {

            responder.sendJson(BAD_REQUEST, Error.serializeErrors(errors, lang));
            return;
        }
        TransferDAO transferDAO = null;
        try {
            transferDAO = DAO_FACTORY.getTransferDAO();
            Transfers transfers = new Transfers(transferDAO.find(filters));
            int exported = transfers.transfers.size();

            XmlUtils.saveObject(transfers, filePath);
            int purged = 0;
            if (purge) {
                for (RestTransfer transfer : transfers.transfers) {
                    transferDAO.delete(transferDAO.select(transfer.transferID));
                    ++purged;
                }
            }
            // Update all UpdatedInfo to DONE
            // where GlobalLastStep = ALLDONETASK and status = CompleteOk
            if (clean) {
                for (Transfer transfer : transfers.transfers) {
                    if (transfer.getGlobalStep() == Transfer.TASKSTEP.ALLDONETASK &&
                            transfer.getInfoStatus() == ErrorCode.CompleteOk) {
                        transfer.setUpdatedInfo(UpdatedInfo.DONE);
                        transferDAO.update(transfer);
                    }
                }
            }

            ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
            responseObject.put("filePath", filePath);
            responseObject.put("exported", exported);
            responseObject.put("purged", purged);
            String responseText = JsonUtils.nodeToString(responseObject);
            responder.sendJson(OK, responseText);

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }
    }

    /**
     * Exports parts of the current server configuration to multiple XML files,
     * depending on the parameters of
     * the request.
     *
     * @param request      The {@link HttpRequest} made on the resource.
     * @param responder    The {@link HttpResponder} which sends the reply to
     *                     the request.
     * @param host_str     HTTP query parameter, states whether to export
     *                     the host database or not.
     * @param rule_str     HTTP query parameter, states whether to export
     *                     the rules database or not.
     * @param business_str HTTP query parameter, states whether to export
     *                     the host's business or not.
     * @param alias_str    HTTP query parameter, states whether to export
     *                     the host's aliases or not.
     * @param role_str     HTTP query parameter, states whether to export
     *                     the host's permission database or not.
     */
    @Path(CONFIG_URI)
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(CONFIGADMIN)
    public void getConfig(HttpRequest request, HttpResponder responder,
                          @QueryParam(EXPORT_HOSTS)
                              @DefaultValue("false") String host_str,
                          @QueryParam(EXPORT_RULES)
                              @DefaultValue("false") String rule_str,
                          @QueryParam(EXPORT_BUSINESS)
                              @DefaultValue("false") String business_str,
                          @QueryParam(EXPORT_ALIASES)
                              @DefaultValue("false") String alias_str,
                          @QueryParam(EXPORT_ROLES)
                              @DefaultValue("false") String role_str) {

        List<Error> errors = new ArrayList<Error>();

        boolean host = false, rule = false, business = false, alias = false, role = false;

        try {
            host = RestUtils.stringToBoolean(host_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_HOSTS, host_str));
        }
        try {
            rule = RestUtils.stringToBoolean(rule_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_RULES, rule_str));
        }
        try {
            business = RestUtils.stringToBoolean(business_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_BUSINESS, business_str));
        }
        try {
            alias = RestUtils.stringToBoolean(alias_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_ALIASES, alias_str));
        }
        try {
            role = RestUtils.stringToBoolean(role_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_ROLES, role_str));
        }
        if (!errors.isEmpty()) {
            Locale lang = RestUtils.getRequestLocale(request);
            String response = Error.serializeErrors(errors, lang);
            responder.sendJson(BAD_REQUEST, response);
            return;
        }

        String hostsFilePath = CONFIGS_PATH + File.separator + SERVER_NAME +
                "_hosts.xml";
        String rulesFilePath = CONFIGS_PATH + File.separator + SERVER_NAME +
                "_rules.xml";
        String businessFilePath = CONFIGS_PATH + File.separator + SERVER_NAME +
                "_business.xml";
        String aliasFilePath = CONFIGS_PATH + File.separator + SERVER_NAME +
                "_aliases.xml";
        String rolesFilePath = CONFIGS_PATH + File.separator + SERVER_NAME +
                "_roles.xml";

        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);

        HostDAO hostDAO = null;
        RuleDAO ruleDAO = null;
        BusinessDAO businessDAO = null;
        try {
            if (host) {
                hostDAO = DAO_FACTORY.getHostDAO();
                List<Host> hostList = hostDAO.getAll();

                Hosts hosts = new Hosts(hostList);

                XmlUtils.saveObject(hosts, hostsFilePath);
                responseObject.put("fileHost", hostsFilePath);
            }
            if (rule) {
                ruleDAO = DAO_FACTORY.getRuleDAO();
                Rules rules = new Rules(ruleDAO.getAll());

                XmlUtils.saveObject(rules, rulesFilePath);
                responseObject.put("fileRule", rulesFilePath);
            }
            businessDAO = DAO_FACTORY.getBusinessDAO();
            Business businessEntry = businessDAO.select(SERVER_NAME);
            if (business) {
                String businessXML = businessEntry.getBusiness();

                XmlUtils.saveXML(businessXML, businessFilePath);
                responseObject.put("fileBusiness", businessFilePath);
            }
            if (alias) {
                String aliasXML = businessEntry.getAliases();

                XmlUtils.saveXML(aliasXML, aliasFilePath);
                responseObject.put("fileAlias", aliasFilePath);
            }
            if (role) {
                String rolesXML = businessEntry.getRoles();

                XmlUtils.saveXML(rolesXML, rolesFilePath);
                responseObject.put("fileRoles", rolesFilePath);
            }

            String responseText = JsonUtils.nodeToString(responseObject);
            responder.sendJson(OK, responseText);

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) { hostDAO.close(); }
            if (ruleDAO != null) { ruleDAO.close(); }
            if (businessDAO != null) { businessDAO.close(); }
        }
    }

    /**
     * Imports different parts of the server configuration from the XML files
     * given as parameters of the request. These imported values will replace
     * those already present in the database.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param purgeHost_str     HTTP query parameter, states if a new host
     *                          database should be imported.
     * @param purgeRule_str     HTTP query parameter, states if a new transfer
     *                          rule database should be imported.
     * @param purgeBusiness_str HTTP query parameter, states if a new business
     *                          database should be imported.
     * @param purgeAlias_str    HTTP query parameter, states if a new alias
     *                          database should be imported.
     * @param purgeRole_str     HTTP query parameter, states if a new role
     *                          database should be imported.
     * @param hostFile          HTTP query parameter, path to the XML file
     *                          containing the host database to import.
     * @param ruleFile          HTTP query parameter, path to the XML file
     *                          containing the rule database to import.
     * @param businessFile           HTTP query parameter, path to the XML file
     *                          containing the business database to import.
     * @param aliasFile         HTTP query parameter, path to the XML file
     *                          containing the alias database to import.
     * @param roleFile          HTTP query parameter, path to the XML file
     *                          containing the role database to import.
     */
    @Path(CONFIG_URI)
    @PUT
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(CONFIGADMIN)
    public void setConfig(HttpRequest request, HttpResponder responder,
                          @QueryParam(PURGE_HOST)
                              @DefaultValue("false") String purgeHost_str,
                          @QueryParam(PURGE_RULE)
                              @DefaultValue("false") String purgeRule_str,
                          @QueryParam(PURGE_BUSINESS)
                              @DefaultValue("false") String purgeBusiness_str,
                          @QueryParam(PURGE_ALIASES)
                              @DefaultValue("false") String purgeAlias_str,
                          @QueryParam(PURGE_ROLES)
                              @DefaultValue("false") String purgeRole_str,
                          @QueryParam(HOST_FILE)
                              @DefaultValue("") String hostFile,
                          @QueryParam(RULE_FILE)
                              @DefaultValue("") String ruleFile,
                          @QueryParam(BUSINESS_FILE)
                              @DefaultValue("") String businessFile,
                          @QueryParam(ALIAS_FILE)
                              @DefaultValue("") String aliasFile,
                          @QueryParam(ROLE_FILE)
                              @DefaultValue("") String roleFile) {

        List<Error> errors = new ArrayList<Error>();
        Locale lang = RestUtils.getRequestLocale(request);

        boolean purgeHost = false, purgeRule = false, purgeBusiness =false,
                purgeAlias = false, purgeRole = false;

        try {
            purgeHost = RestUtils.stringToBoolean(purgeHost_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_HOSTS, purgeHost_str));
        }
        try {
            purgeRule = RestUtils.stringToBoolean(purgeRule_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_RULES, purgeRule_str));
        }
        try {
            purgeBusiness = RestUtils.stringToBoolean(purgeBusiness_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_BUSINESS, purgeBusiness_str));
        }
        try {
            purgeAlias = RestUtils.stringToBoolean(purgeAlias_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_ALIASES, purgeAlias_str));
        }
        try {
            purgeRole = RestUtils.stringToBoolean(purgeRole_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(EXPORT_ROLES, purgeRole_str));
        }
        if (!errors.isEmpty()) {
            String response = Error.serializeErrors(errors, lang);
            responder.sendJson(BAD_REQUEST, response);
            return;
        }

        HostDAO hostDAO = null;
        RuleDAO ruleDAO = null;
        BusinessDAO businessDAO = null;

        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);

        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            Hosts hosts = XmlUtils.loadObject(hostFile, Hosts.class);

            // if a purge is requested, we can add the new entries without
            // checking with 'exist' to gain performance
            if (purgeHost) {
                hostDAO.deleteAll();
                for (Host host : hosts.hosts) {
                    hostDAO.insert(host);
                }
            } else {
                for (Host host : hosts.hosts) {
                    if (hostDAO.exist(host.getHostid())) {
                        hostDAO.update(host);
                    } else {
                        hostDAO.insert(host);
                    }
                }
            }
            responseObject.put("purgedHost", TRUE.toString());

            ruleDAO = DAO_FACTORY.getRuleDAO();

            Rules rules = XmlUtils.loadObject(ruleFile, Rules.class);

            if (purgeRule) {
                ruleDAO.deleteAll();
                for (Rule rule : rules.rules) {
                    ruleDAO.insert(rule);
                }
            } else {
                for (Rule rule : rules.rules) {
                    if (ruleDAO.exist(rule.getName())) {
                        ruleDAO.update(rule);
                    } else {
                        ruleDAO.insert(rule);
                    }
                }
            }

            responseObject.put("purgedRule", TRUE.toString());

            businessDAO = DAO_FACTORY.getBusinessDAO();
            if (purgeBusiness) {
                Business business = businessDAO.select(SERVER_NAME);

                String new_business = XmlUtils.loadXML(businessFile);
                business.setBusiness(new_business);
                businessDAO.update(business);
                responseObject.put("purgedBusiness", TRUE.toString());
            }
            if (purgeAlias) {
                Business business = businessDAO.select(SERVER_NAME);
                business.setAliases(XmlUtils.loadXML(aliasFile));
                businessDAO.update(business);
                responseObject.put("purgedAlias", TRUE.toString());
            }
            if (purgeRole) {
                Business business = businessDAO.select(SERVER_NAME);
                business.setRoles(XmlUtils.loadXML(roleFile));
                businessDAO.update(business);
                responseObject.put("purgedRoles", TRUE.toString());
            }

            if (errors.isEmpty()) {
                responder.sendJson(OK, JsonUtils.nodeToString(responseObject));
            }
            else {
                responder.sendJson(BAD_REQUEST, Error.serializeErrors(errors, lang));
            }

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) { hostDAO.close(); }
            if (ruleDAO != null) { ruleDAO.close(); }
            if (businessDAO != null) { businessDAO.close(); }
        }
    }
}