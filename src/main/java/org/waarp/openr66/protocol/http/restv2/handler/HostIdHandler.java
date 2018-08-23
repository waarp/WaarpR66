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
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestHost;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * This is the handler for all requests made on a single 'host' entry, accessible with the URI "/host/{id}",
 * with {id} being the actual id of the desired host.
 */
@Path("/v2/hosts/{id}")
public class HostIdHandler extends AbstractHttpHandler {

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
            HostDAO dao = RestUtils.factory.getHostDAO();
            Host host = dao.select(id);
            if (host == null) {
                responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
            } else {
                RestHost restHost = new RestHost(host);
                String responseBody = RestUtils.toJsonString(restHost);
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            }
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
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
            RestHost updatedRestHost = RestUtils.deserializeRequest(request, RestHost.class);
            RestUtils.checkEntry(updatedRestHost);
            HostDAO dao = RestUtils.factory.getHostDAO();

            if (dao.exist(id)) {
                dao.update(updatedRestHost.toHost());
                String responseBody = RestUtils.toJsonString(updatedRestHost);
                responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.alreadyExisting("host", id));
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
        } catch (OpenR66RestInvalidEntryException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
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
            HostDAO dao = RestUtils.factory.getHostDAO();
            if(dao.exist(id)) {
                dao.delete(dao.select(id));
                responder.sendStatus(HttpResponseStatus.NO_CONTENT);
            } else {
                responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
            }
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/hosts/{id}. If the request is valid, a response will be
     * sent with a list of allowed http methods in the header. If the request isn't valid the response will contain a
     * '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested host's id, this id is identical to the {id} in the URI of the request.
     */
    @OPTIONS
    public void options(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        HttpHeaders headers = new DefaultHttpHeaders();
        String allow = RestUtils.options(this.getClass());
        headers.add("allow", allow);
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }
}
