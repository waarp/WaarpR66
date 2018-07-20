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

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/** This is the handler for all requests for server commands, accessible through the "/v2/server" URI. */
@Path("/v2/server")
public class ServerHandler extends AbstractHttpHandler {

    /**
     * Get the general status of the server.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("status")
    @GET
    public void getStatus(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.OK);
    }

    /**
     * Shutdown the server.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("shutdown")
    @PUT
    public void shutdown(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.ACCEPTED);
    }

    /**
     * Restart the server.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("restart")
    @PUT
    public void restart(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.ACCEPTED);
    }

    /**
     * Export the server logs to a file.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("logs")
    @GET
    public void getLogs(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.OK);
    }

    /**
     * Export the server configuration to a file.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("config")
    @GET
    public void getConfig(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.OK);
    }

    /**
     * Import the server configuration from a file.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("config")
    @PUT
    public void setConfig(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.ACCEPTED);
    }

    /**
     * Execute a host's business.
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @Path("business")
    @PUT
    public void execBusiness(HttpRequest request, HttpResponder responder){
        responder.sendStatus(HttpResponseStatus.ACCEPTED);
    }
}
