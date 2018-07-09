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
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.openr66.protocol.http.restv2.data.hosts.Host;
import org.waarp.openr66.protocol.http.restv2.data.hosts.Hosts;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.Arrays;

/**
 * This is the handler for all requests made on a single 'host' entry, accessible with the URI "/host/{id}",
 * with {id} being the actual id of the desired host.
 */
@Path("/v2/hosts/{id}")
public class HostIdHandler extends AbstractHttpHandler {

    /** The list of allowed HTTP methods names on the /v2/hosts/{id} URI. Should only be used by the OPTIONS methods. */
    private static final String[] allow = {"GET", "PUT", "PATCH", "DELETE", "OPTIONS"};

    /**
     * The method called when a GET request is made on /v2/hosts/{id}. If the request is valid and the id exists
     * in the database, the Http response will contain the corresponding transfer entry. If the id does not exist,
     * the response will contain a '404 - Not found' error message. If the id exists but the request is invalid, a
     * '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested host's id, this id is identical to the {id} in the URI of the request.
     */
    @GET
    public void getHost(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Host host = Hosts.loadHost(id);
            String responseBody = Hosts.toJsonString(host);
            responder.sendJson(HttpResponseStatus.OK, responseBody);

        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendJson(HttpResponseStatus.NOT_FOUND, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }

    /**
     * The method called when a PUT request is made on /v2/hosts/{id}. If the request is valid and the id exists
     * in the database, the queried host entry will be replaced by the one in the request body and sent back in the Http
     * response. If the id does not exist, the response will be a '404 - Not found' error message. If the id exists but
     * the request is invalid, a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested host's id, this id is identical to the {id} in the URI of the request.
     */
    @PUT
    public void replaceHost(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Host updatedHost = HandlerUtils.deserializeRequest(request, Host.class);
            Hosts.replace(id, updatedHost);

            String responseBody = Hosts.toJsonString(updatedHost);
            responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendJson(HttpResponseStatus.NOT_FOUND, e.message);
        }
    }

    /**
     * The method called when a DELETE request is made on /v2/hosts/{id}. If the request is valid and the id exists
     * in the database, the queried host entry will be removed from the host database. If the id does not exist,
     * the response will be a '404 - Not found' error message. If the id exists but the request is invalid, a
     * '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested host's id, this id is identical to the {id} in the URI of the request.
     */
    @DELETE
    public void deleteHost(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Hosts.deleteHost(id);

            responder.sendStatus(HttpResponseStatus.NO_CONTENT);
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendJson(HttpResponseStatus.NOT_FOUND, e.message);
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/hosts/{id}. If the request is valid, a response will be
     * sent with a list of allowed http methods in the header. If the request isn't valid the response will contain a
     * '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @OPTIONS
    public void options(HttpRequest request, HttpResponder responder) {

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add("allow", Arrays.toString(allow));
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }
}
