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

package org.waarp.openr66.protocol.http.restv2.handler;

import co.cask.http.HttpResponder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolShutdownException;
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestHostConfig;
import org.waarp.openr66.protocol.http.restv2.data.RestTransfer;
import org.waarp.openr66.protocol.http.restv2.data.ServerStatus;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.AuthentPacket;
import org.waarp.openr66.protocol.localhandler.packet.StartupPacket;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is the handler for all requests for server commands, accessible through the "/v2/server" URI. */

@Path("/v2/server")
public class ServerHandler extends AbstractRestHttpHandler {

    public static final Calendar startDate = new GregorianCalendar();

    //TODO: replace by loading the value from server config file
    public static final long period = (long) 1000 * 60 * 60 * 24;

    //TODO: replace by loading argument from server config file
    private static final String archPath = "/arch";

    private static final String logsPath = "/logs";


    public ServerHandler() {
        super(null, null);
    }

    @Override
    public boolean isAuthorized(String user, HttpMethod httpMethod, String method) throws DAOException {
        BusinessDAO businessDAO = RestUtils.factory.getBusinessDAO();
        RestHostConfig.Role[] roles = RestHostConfig.Role.toRoleList(businessDAO.select(RestUtils.HOST_ID).getRoles());
        List<RestHostConfig.RoleType> rights = new ArrayList<RestHostConfig.RoleType>();
        for (RestHostConfig.Role role : roles) {
            if (role.host.equals(user)) {
                rights = Arrays.asList(role.roleTypes);
            }
        }

        if(rights.isEmpty()) {
            return false;
        } else if(rights.contains(RestHostConfig.RoleType.fullAdmin)) {
            return true;
        } else if(method.equals("getStatus")) {
            return rights.contains(RestHostConfig.RoleType.readOnly) ||
                    rights.contains(RestHostConfig.RoleType.partner) ||
                    rights.contains(RestHostConfig.RoleType.configAdmin);
        } else if(method.equals("getLogs")) {
            return rights.contains(RestHostConfig.RoleType.logControl);
        } else {
            return rights.contains(RestHostConfig.RoleType.system);
        }
    }

    /**
     * Get the general status of the server.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("status")
    @GET
    public void getStatus(HttpRequest request, HttpResponder responder) {

        try {
            ServerStatus status = new ServerStatus();
            String jsonString = RestUtils.toJsonString(status);
            responder.sendJson(HttpResponseStatus.OK, jsonString);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
        }
    }

    /**
     * Shutdown the server.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("shutdown")
    @PUT
    public void shutdown(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
    }

    /**
     * Restart the server.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("restart")
    @PUT
    public void restart(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
    }

    /**
     * Export the server logs to a file. Only the entries that satisfy the desired filters will be exported.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param purge     Whether to delete exported entries or not.
     * @param clean     Whether to fix the incoherent entries.
     * @param status    Export only the entries with one of these statutes.
     * @param rule      Export only the entries using this rule.
     * @param start     Lower bound for the date of the transfer.
     * @param stop      Upper bound for the data of the transfer.
     * @param startID   Lower bound for the transfer ID.
     * @param stopID    Upper bound for the transfer ID.
     */
    @Path("logs")
    @GET
    public void getLogs(HttpRequest request, HttpResponder responder,
                        @QueryParam("purge") @DefaultValue("false") Boolean purge,
                        @QueryParam("clean") @DefaultValue("false") Boolean clean,
                        @QueryParam("status") List<RestTransfer.Status> status,
                        @QueryParam("rule") String rule,
                        @QueryParam("start") String start,
                        @QueryParam("stop") String stop,
                        @QueryParam("startID") String startID,
                        @QueryParam("stopID") String stopID) {

        Map<String, List<String>> query = (new QueryStringDecoder(request.uri())).parameters();

        try {
            //TODO: make a request on the transfers database

            Map<String, String> response = new HashMap<String, String>();
            response.put("filePath", logsPath + "/transfers.log");
            response.put("exported", String.valueOf(0));
            response.put("purged", String.valueOf(0));

            String jsonString;
            ObjectMapper mapper = new ObjectMapper();
            jsonString = mapper.writeValueAsString(response);
            responder.sendJson(HttpResponseStatus.OK, jsonString);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        }
    }

    /**
     * Export the server configuration to a file.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param hosts     Whether to export the host database or not.
     * @param rules     Whether to export the rules database or not.
     * @param business  Whether to export the host's business or not.
     * @param aliases   Whether to export the host's aliases or not.
     * @param roles     Whether to export the host's permission database or not.
     */
    @Path("config")
    @GET
    public void getConfig(HttpRequest request, HttpResponder responder,
                          @QueryParam("hosts") @DefaultValue("false") Boolean hosts,
                          @QueryParam("rules") @DefaultValue("false") Boolean rules,
                          @QueryParam("business") @DefaultValue("false") Boolean business,
                          @QueryParam("aliases") @DefaultValue("false") Boolean aliases,
                          @QueryParam("roles") @DefaultValue("false") Boolean roles) {

        Map<String, List<String>> query = (new QueryStringDecoder(request.uri())).parameters();
        Map<String, String> files = new HashMap<String, String>();

        try {
            if (hosts) {
                //TODO: save hosts to file
                files.put("hostFile", archPath + "/hosts");
            }
            if (rules) {
                //TODO: save rules to file
                files.put("ruleFile", archPath + "/rules");
            }
            if (business) {
                //TODO: save business to file
                files.put("businessFile", archPath + "/business");
            }
            if (aliases) {
                //TODO: save aliases to file
                files.put("aliasFile", archPath + "/aliases");
            }
            if (roles) {
                //TODO: save roles to file
                files.put("roleFile", archPath + "/roles");
            }
            String jsonString;
            ObjectMapper mapper = new ObjectMapper();

            jsonString = mapper.writeValueAsString(files);
            responder.sendJson(HttpResponseStatus.OK, jsonString);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        }
    }

    /**
     * Import the server configuration from a file.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("config")
    @PUT
    public void setConfig(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
    }

    /**
     * Execute a host's business.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("business")
    @PUT
    public void execBusiness(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
    }
}