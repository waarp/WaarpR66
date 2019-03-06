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

import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.joda.time.DateTime;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestHost;
import org.waarp.openr66.protocol.http.restv2.data.RestRule;
import org.waarp.openr66.protocol.http.restv2.data.RestTransfer;
import org.waarp.openr66.protocol.http.restv2.data.ServerStatus;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
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

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.lang.Boolean.TRUE;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.CONFIGADMIN;
import static org.waarp.common.role.RoleDefault.ROLE.FULLADMIN;
import static org.waarp.common.role.RoleDefault.ROLE.LOGCONTROL;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.READONLY;
import static org.waarp.gateway.kernel.rest.RestConfiguration.CRUD;
import static org.waarp.openr66.dao.database.DBTransferDAO.ID_FIELD;
import static org.waarp.openr66.dao.database.DBTransferDAO.ID_RULE_FIELD;
import static org.waarp.openr66.dao.database.DBTransferDAO.REQUESTER_FIELD;
import static org.waarp.openr66.dao.database.DBTransferDAO.TRANSFER_START_FIELD;
import static org.waarp.openr66.dao.database.DBTransferDAO.UPDATED_INFO_FIELD;
import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.Business;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.Config;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.Information;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.Log;
import static org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS.Server;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.SERVER_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.errors.Error.serializeErrors;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ILLEGAL_PARAMETER_VALUE;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.stringToBoolean;
import static org.waarp.openr66.protocol.http.restv2.utils.XmlUtils.loadObjectFromXmlFile;
import static org.waarp.openr66.protocol.http.restv2.utils.XmlUtils.loadXmlFileToString;
import static org.waarp.openr66.protocol.http.restv2.utils.XmlUtils.saveObjectToXmlFile;
import static org.waarp.openr66.protocol.http.restv2.utils.XmlUtils.saveStringToXmlFile;

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
    private static final class EntryPoints {
        static final String STATUS_URI = "status";
        static final String DEACTIVATE_URI = "deactivate";
        static final String SHUTDOWN_URI = "shutdown";
        static final String RESTART_URI = "restart";
        static final String LOGS_URI = "logs";
        static final String CONFIG_URI = "config";
        static final String BUSINESS_URI = "business";
    }

    private static final class Params {
        static final class GetLogs {
            static final String purge = "purge";
            static final String clean = "clean";
            static final String status = "status";
            static final String rule = "rule";
            static final String start = "start";
            static final String stop = "stop";
            static final String startID = "startID";
            static final String stopID = "stopID";
            static final String requester = "requester";
        }
        static final class GetConfig {
            static final String host = "host";
            static final String rule = "rule";
            static final String business = "business";
            static final String alias = "alias";
            static final String role = "role";
        }
        static final class SetConfig {
            static final String purgeHost = "purgeHost";
            static final String purgeRule = "purgeRule";
            static final String purgeBusiness = "purgeBusiness";
            static final String purgeAlias = "purgeAlias";
            static final String purgeRole = "purgeRole";
            static final String hostFile = "hostFile";
            static final String ruleFile = "ruleFile";
            static final String businessFile = "businessFile";
            static final String aliasFile = "aliasFile";
            static final String roleFile = "roleFile";
        }
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
     * Stores a {@link Map} associating each subpath of the handler to their
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
        serverCRUD.put(EntryPoints.STATUS_URI, crud[Information.ordinal()]);
        serverCRUD.put(EntryPoints.DEACTIVATE_URI, crud[Server.ordinal()]);
        serverCRUD.put(EntryPoints.SHUTDOWN_URI, crud[Server.ordinal()]);
        serverCRUD.put(EntryPoints.RESTART_URI, crud[Server.ordinal()]);
        serverCRUD.put(EntryPoints.LOGS_URI, crud[Log.ordinal()]);
        serverCRUD.put(EntryPoints.CONFIG_URI, crud[Config.ordinal()]);
        serverCRUD.put(EntryPoints.BUSINESS_URI, crud[Business.ordinal()]);
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
    @Path(EntryPoints.STATUS_URI)
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(READONLY)
    public void getStatus(HttpRequest request, HttpResponder responder) {
        ServerStatus status = new ServerStatus();
        String jsonString = objectToJson(status);
        responder.sendJson(OK, jsonString);
    }

    /**
     * Deactivates the server so that it doesn't accept any new transfer request.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(EntryPoints.DEACTIVATE_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(FULLADMIN)
    public void deactivate(HttpRequest request, HttpResponder responder) {
        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            Host host = hostDAO.select(HOST_ID);
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
    @Path(EntryPoints.SHUTDOWN_URI)
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
    @Path(EntryPoints.RESTART_URI)
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
    @Path(EntryPoints.LOGS_URI)
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(LOGCONTROL)
    public void getLogs(HttpRequest request, HttpResponder responder,
                        @QueryParam(Params.GetLogs.purge)
                            @DefaultValue("false") String purge_str,
                        @QueryParam(Params.GetLogs.clean)
                            @DefaultValue("false") String clean_str,
                        @QueryParam(Params.GetLogs.status)
                                    List<String> status_str,
                        @QueryParam(Params.GetLogs.rule)
                            @DefaultValue("") String rule,
                        @QueryParam(Params.GetLogs.start)
                            @DefaultValue("") String start,
                        @QueryParam(Params.GetLogs.stop)
                            @DefaultValue("") String stop,
                        @QueryParam(Params.GetLogs.startID)
                            @DefaultValue("") String startID,
                        @QueryParam(Params.GetLogs.stopID)
                            @DefaultValue("") String stopID,
                        @QueryParam(Params.GetLogs.requester)
                            @DefaultValue("") String requester) {

        List<Error> errors = new ArrayList<Error>();
        Locale lang = getRequestLocale(request);
        List<Filter> filters = new ArrayList<Filter>();
        String filePath = ARCH_PATH + File.separator + HOST_ID +
                "_export_" + DateTime.now().toString() + ".log";


        Boolean purge = false, clean = false;
        try {
            purge = stringToBoolean(purge_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.purge, purge_str));
        }
        try {
            clean = stringToBoolean(clean_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.clean, clean_str));
        }

        if (!start.isEmpty()) {
            try {
                DateTime lowerDate = DateTime.parse(start);
                filters.add(new Filter(TRANSFER_START_FIELD, ">=", lowerDate));
            } catch(IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.start, start));
            }
        }
        if (!stop.isEmpty()) {
            try {
                DateTime upperDate = DateTime.parse(stop);
                filters.add(new Filter(TRANSFER_START_FIELD, "<=", upperDate));
            } catch(IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.stop, stop));
            }
        }
        if (!startID.isEmpty()) {
            try {
                Long lowerID = Long.parseLong(startID);
                filters.add(new Filter(ID_FIELD, ">=", lowerID));
            } catch (NumberFormatException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.startID, startID));
            }
        }
        if (!stopID.isEmpty()) {
            try {
                Long upperID = Long.parseLong(stopID);
                filters.add(new Filter(ID_FIELD, "<=", upperID));
            } catch (NumberFormatException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.stopID, stopID));
            }
        }
        if (!rule.isEmpty()) {
            filters.add(new Filter(ID_RULE_FIELD, "=", rule));
        }
        if (!status_str.isEmpty()) {
            for (String s : status_str) {
                try {
                    RestTransfer.Status status = RestTransfer.Status.valueOf(s);
                    filters.add(new Filter(UPDATED_INFO_FIELD, "=", status.toUpdatedInfo()));
                } catch (IllegalArgumentException e) {
                    errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetLogs.status, s));
                }
            }
        }

        if (!requester.isEmpty()) {
            filters.add(new Filter(REQUESTER_FIELD, "=", requester));
        }

        if (!errors.isEmpty()) {

            responder.sendJson(BAD_REQUEST, serializeErrors(errors, lang));
            return;
        }
        TransferDAO transferDAO = null;
        try {
            transferDAO = DAO_FACTORY.getTransferDAO();
            RestTransfer.RestTransferList transfers = new RestTransfer.RestTransferList();
            transfers.transfers = RestTransfer.toRestList(transferDAO.find(filters));
            int exported = transfers.transfers.size();

            saveObjectToXmlFile(transfers, filePath);
            int purged = 0;
            if (purge) {
                for (RestTransfer transfer : transfers.transfers) {
                    transferDAO.delete(transferDAO.select(transfer.transferID));
                    ++purged;
                }
            }

            HashMap<String, Object> jsonObject = new HashMap<String, Object>();
            jsonObject.put("filePath", filePath);
            jsonObject.put("exported", exported);
            jsonObject.put("purged", purged);
            String jsonBody = objectToJson(jsonObject);
            responder.sendJson(OK, jsonBody);

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
    @Path(EntryPoints.CONFIG_URI)
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(CONFIGADMIN)
    public void getConfig(HttpRequest request, HttpResponder responder,
                          @QueryParam(Params.GetConfig.host)
                              @DefaultValue("false") String host_str,
                          @QueryParam(Params.GetConfig.rule)
                              @DefaultValue("false") String rule_str,
                          @QueryParam(Params.GetConfig.business)
                              @DefaultValue("false") String business_str,
                          @QueryParam(Params.GetConfig.alias)
                              @DefaultValue("false") String alias_str,
                          @QueryParam(Params.GetConfig.role)
                              @DefaultValue("false") String role_str) {

        List<Error> errors = new ArrayList<Error>();

        boolean host = false, rule = false, business = false, alias = false, role = false;

        try {
            host = stringToBoolean(host_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.host, host_str));
        }
        try {
            rule = stringToBoolean(rule_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.rule, rule_str));
        }
        try {
            business = stringToBoolean(business_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.business, business_str));
        }
        try {
            alias = stringToBoolean(alias_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.alias, alias_str));
        }
        try {
            role = stringToBoolean(role_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.role, role_str));
        }
        if (!errors.isEmpty()) {
            Locale lang = getRequestLocale(request);
            String response = serializeErrors(errors, lang);
            responder.sendJson(BAD_REQUEST, response);
            return;
        }

        String hostsFilePath = CONFIGS_PATH + File.separator + HOST_ID +
                "_hosts.xml";
        String rulesFilePath = CONFIGS_PATH + File.separator + HOST_ID +
                "_rules.xml";
        String aliasFilePath = CONFIGS_PATH + File.separator + HOST_ID +
                "_aliases.xml";
        String rolesFilePath = CONFIGS_PATH + File.separator + HOST_ID +
                "_roles.xml";

        HashMap<String, String> jsonObject = new HashMap<String, String>();

        HostDAO hostDAO = null;
        RuleDAO ruleDAO = null;
        BusinessDAO businessDAO = null;
        try {
            if (host) {
                hostDAO = DAO_FACTORY.getHostDAO();
                RestHost.RestHostList hostList = new RestHost.RestHostList();
                hostList.hosts = RestHost.toRestList(hostDAO.getAll());

                saveObjectToXmlFile(hostList, hostsFilePath);
                jsonObject.put("fileHost", hostsFilePath);
            }
            if (rule) {
                ruleDAO = DAO_FACTORY.getRuleDAO();
                RestRule.RestRuleList ruleList = new RestRule.RestRuleList();
                ruleList.rules = RestRule.toRestList(ruleDAO.getAll());

                saveObjectToXmlFile(ruleList, rulesFilePath);
                jsonObject.put("fileRule", rulesFilePath);
            }
            if (business) {
                //TODO : collect host's business and export to XML file
            }
            businessDAO = DAO_FACTORY.getBusinessDAO();
            if (alias) {
                Business businessEntry = businessDAO.select(HOST_ID);
                String aliasXML = businessEntry.getAliases();

                saveStringToXmlFile(aliasXML, aliasFilePath);
                jsonObject.put("fileAlias", aliasFilePath);
            }
            if (role) {
                Business businessEntry = businessDAO.select(HOST_ID);
                String rolesXML = businessEntry.getRoles();

                saveStringToXmlFile(rolesXML, rolesFilePath);
                jsonObject.put("fileRoles", rolesFilePath);
            }

            responder.sendJson(OK, objectToJson(jsonObject));

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
    @Path(EntryPoints.CONFIG_URI)
    @PUT
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(CONFIGADMIN)
    public void setConfig(HttpRequest request, HttpResponder responder,
                          @QueryParam(Params.SetConfig.purgeHost)
                              @DefaultValue("false") String purgeHost_str,
                          @QueryParam(Params.SetConfig.purgeRule)
                              @DefaultValue("false") String purgeRule_str,
                          @QueryParam(Params.SetConfig.purgeBusiness)
                              @DefaultValue("false") String purgeBusiness_str,
                          @QueryParam(Params.SetConfig.purgeAlias)
                              @DefaultValue("false") String purgeAlias_str,
                          @QueryParam(Params.SetConfig.purgeRole)
                              @DefaultValue("false") String purgeRole_str,
                          @QueryParam(Params.SetConfig.hostFile)
                              @DefaultValue("") String hostFile,
                          @QueryParam(Params.SetConfig.ruleFile)
                              @DefaultValue("") String ruleFile,
                          @QueryParam(Params.SetConfig.businessFile)
                              @DefaultValue("") String businessFile,
                          @QueryParam(Params.SetConfig.aliasFile)
                              @DefaultValue("") String aliasFile,
                          @QueryParam(Params.SetConfig.roleFile)
                              @DefaultValue("") String roleFile) {

        List<Error> errors = new ArrayList<Error>();
        Locale lang = getRequestLocale(request);

        boolean purgeHost = false, purgeRule = false, purgeBusiness =false,
                purgeAlias = false, purgeRole = false;

        try {
            purgeHost = stringToBoolean(purgeHost_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.host, purgeHost_str));
        }
        try {
            purgeRule = stringToBoolean(purgeRule_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.rule, purgeRule_str));
        }
        try {
            purgeBusiness = stringToBoolean(purgeBusiness_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.business, purgeBusiness_str));
        }
        try {
            purgeAlias = stringToBoolean(purgeAlias_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.alias, purgeAlias_str));
        }
        try {
            purgeRole = stringToBoolean(purgeRole_str);
        } catch (ParseException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.GetConfig.role, purgeRole_str));
        }
        if (!errors.isEmpty()) {
            String response = serializeErrors(errors, lang);
            responder.sendJson(BAD_REQUEST, response);
            return;
        }

        HostDAO hostDAO = null;
        RuleDAO ruleDAO = null;
        BusinessDAO businessDAO = null;

        HashMap<String, String> jsonObject = new HashMap<String, String>();

        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            RestHost.RestHostList hosts = loadObjectFromXmlFile(hostFile,
                    RestHost.RestHostList.class);

            // if a purge is requested, we can add the new entries without
            // checking with 'exist' to gain performance
            if (purgeHost) {
                hostDAO.deleteAll();
                for (RestHost host : hosts.hosts) {
                    hostDAO.insert(host.toHost());
                }
            } else {
                for (RestHost host : hosts.hosts) {
                    if (hostDAO.exist(host.hostID)) {
                        hostDAO.update(host.toHost());
                    } else {
                        hostDAO.insert(host.toHost());
                    }
                }
            }
            jsonObject.put("purgedHost", TRUE.toString());

            ruleDAO = DAO_FACTORY.getRuleDAO();
            RestRule.RestRuleList rules = loadObjectFromXmlFile(
                    ruleFile, RestRule.RestRuleList.class);
            if (purgeRule) {
                ruleDAO.deleteAll();
                for (RestRule rule : rules.rules) {
                    ruleDAO.insert(rule.toRule());
                }
            } else {
                for (RestRule rule : rules.rules) {
                    if (ruleDAO.exist(rule.ruleID)) {
                        ruleDAO.update(rule.toRule());
                    } else {
                        ruleDAO.insert(rule.toRule());
                    }
                }
            }
            jsonObject.put("purgedRule", TRUE.toString());

            businessDAO = DAO_FACTORY.getBusinessDAO();
            if (purgeBusiness) {
                Business business = businessDAO.select(HOST_ID);

                String new_business = loadXmlFileToString(businessFile);
                business.setBusiness(new_business);
                businessDAO.update(business);
                jsonObject.put("purgedBusiness", TRUE.toString());
            }
            if (purgeAlias) {
                Business business = businessDAO.select(HOST_ID);
                business.setAliases(loadXmlFileToString(aliasFile));
                businessDAO.update(business);
                jsonObject.put("purgedAlias", TRUE.toString());
            }
            if (purgeRole) {
                Business business = businessDAO.select(HOST_ID);
                business.setRoles(loadXmlFileToString(roleFile));
                businessDAO.update(business);
                jsonObject.put("purgedRoles", TRUE.toString());
            }

            if (errors.isEmpty()) {
                responder.sendJson(OK, objectToJson(jsonObject));
            }
            else {
                responder.sendJson(BAD_REQUEST, serializeErrors(errors, lang));
            }

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) { hostDAO.close(); }
            if (ruleDAO != null) { ruleDAO.close(); }
            if (businessDAO != null) { businessDAO.close(); }
        }
    }

    /**
     * Execute a host's business.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @Path(EntryPoints.BUSINESS_URI)
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void getBusiness(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(NOT_IMPLEMENTED);
    }
}