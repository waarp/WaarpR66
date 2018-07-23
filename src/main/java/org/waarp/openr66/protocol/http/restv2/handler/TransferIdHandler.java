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
import org.waarp.openr66.protocol.http.restv2.data.transfers.Transfer;
import org.waarp.openr66.protocol.http.restv2.data.transfers.Transfers;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * This is the handler for all requests made on a single 'transfer' entry, accessible with the URI "/transfer/{id}",
 * with {id} being the actual id of the desired transfer.
 */
@Path("/v2/transfers/{id}")
public class TransferIdHandler extends AbstractHttpHandler {

    /**
     * The list of allowed HTTP methods names on the /v2/transfers/{id} URI. Should only be used by the OPTIONS
     * methods.
     */
    private static final String allow = "GET, OPTIONS";

    /**
     * The method called when a GET request is made on /v2/transfers/{id}. If the request is valid and the id exists
     * in the database, the Http response will contain the corresponding transfer entry. If the id does not exist,
     * the response will contain a '404 - Not found' error message. If the id exists but the request is invalid, a
     * '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested transfer's id, this id is identical to the {id} in the URI of the request.
     */
    @GET
    public void getTransfer(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Transfer trans = Transfers.loadTransfer(id);
            String responseBody = Transfers.toJsonString(trans);
            responder.sendJson(HttpResponseStatus.OK, responseBody);

        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/transfers/{id}. If the request is valid, a response
     * will be sent with a list of allowed http methods in the header. If the request isn't valid the response will
     * contain a '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested transfer's id, this id is identical to the {id} in the URI of the request.
     */
    @OPTIONS
    public void options(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add("allow", allow);
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }

    /**
     * The method called when a PUT request is made on /v2/transfers/{id}/restart. If the request is valid and the
     * id exists in the database, the requested transfer will be restarted, and the Http response will contain the
     * corresponding updated transfer entry. If the id does not exist, the response will contain a '404 - Not found'
     * error message. If the id exists but the request is invalid, a '400 - Bad request' error will be sent
     * instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested transfer's id, this id is identical to the {id} in the URI of the request.
     */
    @Path("restart")
    @PUT
    public void restartTransfer(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Transfer trans = Transfers.loadTransfer(id);
            Transfers.restart(trans);
            String responseBody = Transfers.toJsonString(trans);
            responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);

        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }

    /**
     * The method called when a PUT request is made on /v2/transfers/{id}/stop. If the request is valid and the id
     * exists in the database, the requested transfer will be paused, and the Http response will contain the
     * corresponding updated transfer entry. If the id does not exist, the response will contain a '404 - Not found'
     * error message. If the id exists but the request is invalid, a '400 - Bad request' error will be sent
     * instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested transfer's id, this id is identical to the {id} in the URI of the request.
     */
    @Path("stop")
    @PUT
    public void stopTransfer(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Transfer trans = Transfers.loadTransfer(id);
            Transfers.stop(trans);
            String responseBody = Transfers.toJsonString(trans);
            responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);

        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }

    /**
     * The method called when a PUT request is made on /v2/transfers/{id}/cancel. If the request is valid and the id
     * exists in the database, the requested transfer will be restarted, and the Http response will contain the
     * corresponding updated transfer entry. If the id does not exist, the response will contain a '404 - Not found'
     * error message. If the id exists but the request is invalid, a '400 - Bad request' error will be sent
     * instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested transfer's id, this id is identical to the {id} in the URI of the request.
     */
    @Path("cancel")
    @PUT
    public void cancelTransfer(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Transfer trans = Transfers.loadTransfer(id);
            Transfers.cancel(trans);
            String responseBody = Transfers.toJsonString(trans);
            responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);

        } catch (OpenR66RestIdNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }
}
