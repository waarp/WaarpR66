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

package org.waarp.openr66.protocol.http.restv2.handler;

import co.cask.http.HttpResponder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestTransfer;
import org.waarp.openr66.protocol.http.restv2.data.ServerStatus;
import org.waarp.openr66.protocol.http.restv2.errors.InternalErrorResponse;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/** This is the handler for all requests for server commands, accessible through the "/v2/server" URI. */

@Path("/v2/server")
public class ServerHandler extends AbstractRestHttpHandler {

    public static final Calendar startDate = new GregorianCalendar();

    public static final long period = Configuration.configuration.getTimeLimitCache();

    private static final String archPath = "/arch";

    private static final String logsPath = "/logs";


    public ServerHandler() {
        super(RoleDefault.ROLE.SYSTEM);
    }

    @Override
    public boolean isAuthorized(HttpMethod method, R66Auth auth, String uri) {
        if(uri.equals(this.getClass().getAnnotation(Path.class).value() + "logs")) {
            if(method == HttpMethod.GET) {
                return auth.isValidRole(RoleDefault.ROLE.LOGCONTROL);
            } else {
                return auth.isValidRole(RoleDefault.ROLE.LOGCONTROL) && !auth.getRole().hasReadOnly();
            }
        } else {
            return super.isAuthorized(method, auth, uri);
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
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.jsonProcessingError().toJson());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
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
        R66ShutdownHook.setRestart(false);
        R66ShutdownHook.shutdownWillStart();
        ChannelUtils.startShutdown();
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
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
        R66ShutdownHook.setRestart(true);
        R66ShutdownHook.shutdownWillStart();
        ChannelUtils.startShutdown();
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
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

        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
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

        responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
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