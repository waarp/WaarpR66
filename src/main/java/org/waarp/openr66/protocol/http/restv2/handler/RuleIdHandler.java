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
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.rules.Rule;
import org.waarp.openr66.protocol.http.restv2.data.rules.Rules;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import javax.ws.rs.*;
import java.util.Arrays;

/**
 * This is the handler for all requests made on a single 'rule' entry, accessible with the URI "/rule/{id}",
 * with {id} being the actual id of the desired rule.
 */
@Path("/v2/rules/{id}")
public class RuleIdHandler extends AbstractHttpHandler {

    /** The list of allowed HTTP methods names on the /v2/rules/{id} URI. Should only be used by the OPTIONS methods. */
    private static final String[] allow = {"GET", "PUT", "PATCH", "DELETE", "OPTIONS"};

    /**
     * The method called when a GET request is made on /v2/rules/{id}. If the request is valid and the id exists
     * in the database, the Http response will contain the corresponding transfer entry. If the id does not exist,
     * the response will contain a '404 - Not found' error message. If the id exists but the request is invalid, a
     * '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested rule's id, this id is identical to the {id} in the URI of the request.
     */
    @GET
    public void getRule(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Rule rule = Rules.loadRule(id);
            String responseBody = Rules.toJsonString(rule);
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
     * The method called when a PUT request is made on /v2/rules/{id}. If the request is valid and the id exists
     * in the database, the queried rule entry will be replaced by the one in the request body and sent back in the Http
     * response. If the id does not exist, the response will be a '404 - Not found' error message. If the id exists but
     * the request is invalid, a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested rule's id, this id is identical to the {id} in the URI of the request.
     */
    @PUT
    public void replaceRule(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Rule updatedRule = RestUtils.deserializeRequest(request, Rule.class);
            Rules.replace(id, updatedRule);

            String responseBody = Rules.toJsonString(updatedRule);
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
     * The method called when a PATCH request is made on /v2/rules/{id}. If the request is valid and the id exists
     * in the database, the queried rule entry will be replaced by the one in the request body and sent back in the Http
     * response. All fields left empty in the request will be left unchanged with their old values. If the id does not
     * exist, the response will be a '404 - Not found' error message. If the id exists but the request is invalid,
     * a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested rule's id, this id is identical to the {id} in the URI of the request.
     */
    @PATCH
    public void updateRule(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Rule updatedRule = RestUtils.deserializeRequest(request, Rule.class);
            Rules.update(id, updatedRule);

            String responseBody = Rules.toJsonString(updatedRule);
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
     * The method called when a DELETE request is made on /v2/rules/{id}. If the request is valid and the id exists
     * in the database, the queried rule entry will be removed from the rule database. If the id does not exist,
     * the response will be a '404 - Not found' error message. If the id exists but the request is invalid, a
     * '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param id        The requested rule's id, this id is identical to the {id} in the URI of the request.
     */
    @DELETE
    public void deleteRule(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
        try {
            Rules.deleteRule(id);

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
     * The method called when an OPTIONS request is made on /v2/rules/{id}. If the request is valid, a response will be
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