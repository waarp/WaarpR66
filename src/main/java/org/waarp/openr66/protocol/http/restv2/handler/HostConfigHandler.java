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
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.HostConfig;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;
import org.waarp.openr66.protocol.http.restv2.testdatabases.HostconfigDatabase;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * This is the handler for all request made on the 'hostconfig' database, accessible through the "/v2/hostconfig" URI.
 */
@Path("/v2/hostconfig")
public class HostConfigHandler extends AbstractHttpHandler {

    /**
     * The method called when a GET request is made on /v2/hostconfig. If the request is valid and the host does
     * have a configuration, the Http response will contain the corresponding host config. If the host does not have a
     * configuration, the response will contain a '404 - Not found' error message. If the host has a config but the
     * request is invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @GET
    public void getConfig(HttpRequest request, HttpResponder responder) {
        HostConfig config = HostconfigDatabase.select(RestUtils.HOST_ID);

        if(config != null) {
            try {
                String responseBody = RestUtils.toJsonString(config);
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            } catch (JsonProcessingException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
            }
        } else {
            responder.sendJson(HttpResponseStatus.NOT_FOUND, request.uri());
        }
    }

    /**
     * The method called when a POST request is made on /v2/hostconfig. If the request is valid and the host does
     * not already have a configuration, then the config will be added to the database, and the Http response will
     * contain the corresponding host config. If the host does already have a configuration, or if the request is
     * invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @POST
    public void initializeConfig(HttpRequest request, HttpResponder responder) {

        try {
            HostConfig config = RestUtils.deserializeRequest(request, HostConfig.class);
            RestUtils.checkEntry(config);
            config.defaultValues();

            if(HostconfigDatabase.insert(config)) {
                String responseBody = RestUtils.toJsonString(config);
                responder.sendJson(HttpResponseStatus.CREATED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                        RestResponses.alreadyInitialized("host configuration"));
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        } catch (OpenR66RestInvalidEntryException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        }
    }

    /**
     * The method called when a PUT request is made on /v2/hostconfig. If the request is valid and the host does
     * already have a configuration, then the config will be replaced by the one in the request, and the updated
     * configuration will be sent in the response. If the host does not have a configuration to replace, or if the
     * request is invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @PUT
    public void replaceConfig(HttpRequest request, HttpResponder responder) {
        try {
            HostConfig updatedConfig = RestUtils.deserializeRequest(request, HostConfig.class);

            if(HostconfigDatabase.modify(RestUtils.HOST_ID, updatedConfig)) {
                String responseBody = RestUtils.toJsonString(updatedConfig);
                responder.sendJson(HttpResponseStatus.ACCEPTED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                        RestResponses.alreadyInitialized("host configuration"));
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        }
    }

    /**
     * The method called when a DELETE request is made on /v2/hostconfig. If the request is valid and the does have a
     * configuration, the queried host entry will be deleted from the database. If the host does not have a
     * configuration or if the request is invalid, a '400 - Bad request' error will be sent in response.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @DELETE
    public void deleteConfig(HttpRequest request, HttpResponder responder) {
        if(HostconfigDatabase.delete(RestUtils.HOST_ID)) {
            responder.sendStatus(HttpResponseStatus.NO_CONTENT);
        } else {
            responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/hostconfig. If the request is valid, a response will be
     * sent with a list of allowed http methods in the header. If the request isn't valid the response will contain a
     * '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @OPTIONS
    public void options(HttpRequest request, HttpResponder responder) {
        HttpHeaders headers = new DefaultHttpHeaders();
        String allow = RestUtils.options(this.getClass());
        headers.add("allow", allow);
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }
}

