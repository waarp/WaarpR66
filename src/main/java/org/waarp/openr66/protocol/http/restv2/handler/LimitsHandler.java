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
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.dao.LimitDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestHostConfig;
import org.waarp.openr66.protocol.http.restv2.data.RestLimit;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestResponse;
import org.waarp.openr66.protocol.http.restv2.errors.InternalErrorResponse;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.util.Arrays;
import java.util.List;

/**
 * This is the handler for all request made on the 'limits' database, accessible through the "/v2/limits" URI.
 */
@Path("/v2/limits")
public class LimitsHandler extends AbstractRestHttpHandler {

    public LimitsHandler() {
        super(RoleDefault.ROLE.LIMIT);
    }

    /**
     * The method called when a GET request is made on /v2/limits. If the request is valid and the host does
     * have bandwidth limits, the Http response will contain the corresponding host config. If the host does not have
     * any bandwidth limits, the response will contain a '404 - Not found' error message. If the host has limits but
     * the request is invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @GET
    public void getLimits(HttpRequest request, HttpResponder responder) {
        try {
            LimitDAO limitDAO = RestUtils.factory.getLimitDAO();
            if (limitDAO.exist(RestUtils.HOST_ID)) {
                RestLimit restLimit = new RestLimit(limitDAO.select(RestUtils.HOST_ID));
                String responseBody = RestUtils.toJsonString(restLimit);
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            } else {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            }
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.jsonProcessingError().toJson());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
        }
    }

    /**
     * The method called when a POST request is made on /v2/limits. If the request is valid and the host does
     * not already have bandwidth limits, then the config will be added to the database, and the Http response will
     * contain the corresponding host config. If the host does already have bandwidth limits, or if the request is
     * invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @POST
    public void initializeLimits(HttpRequest request, HttpResponder responder) {
        try {
            RestLimit restLimit = RestUtils.deserializeRequest(request, RestLimit.class);
            RestUtils.checkEntry(restLimit);

            LimitDAO limitDAO = RestUtils.factory.getLimitDAO();

            if(!limitDAO.exist(RestUtils.HOST_ID)) {
                limitDAO.insert(restLimit.toLimit());
                String responseBody = RestUtils.toJsonString(restLimit);
                responder.sendJson(HttpResponseStatus.CREATED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                        new BadRequestResponse().alreadyInitialized(RestLimit.class).toJson());
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.toJson());
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toJson());
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.jsonProcessingError().toJson());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
        }
    }

    /**
     * The method called when a PUT request is made on /v2/limits. If the request is valid and the host does
     * already have bandwidth limits, then the config will be replaced by the one in the request, and the updated
     * limits will be sent in the response. If the host does not have bandwidth limits to replace, or if the
     * request is invalid, then a '400 - Bad request' error will be sent instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @PUT
    public void replaceLimits(HttpRequest request, HttpResponder responder) {
        try {
            RestLimit restLimit = RestUtils.deserializeRequest(request, RestLimit.class);
            RestUtils.checkEntry(restLimit);

            LimitDAO limitDAO = RestUtils.factory.getLimitDAO();

            if(limitDAO.exist(RestUtils.HOST_ID)) {
                limitDAO.delete(limitDAO.select(RestUtils.HOST_ID));
                limitDAO.insert(restLimit.toLimit());
                String responseBody = RestUtils.toJsonString(restLimit);
                responder.sendJson(HttpResponseStatus.CREATED, responseBody);
            } else {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.toJson());
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toJson());
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.jsonProcessingError().toJson());
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
        }
    }

    /**
     * The method called when a DELETE request is made on /v2/limits. If the request is valid and the does have
     * bandwidth limits, the queried host entry will be deleted from the database. If the host does not have bandwidth
     * limits or if the request is invalid, a '400 - Bad request' error will be sent in response.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @DELETE
    public void deleteLimits(HttpRequest request, HttpResponder responder) {
        try {
            LimitDAO limitDAO = RestUtils.factory.getLimitDAO();

            if (limitDAO.exist(RestUtils.HOST_ID)) {
                limitDAO.delete(limitDAO.select(RestUtils.HOST_ID));
                responder.sendStatus(HttpResponseStatus.NO_CONTENT);
            } else {
                responder.sendString(HttpResponseStatus.NOT_FOUND, request.uri());
            }
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/limits. If the request is valid, a response will be
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
