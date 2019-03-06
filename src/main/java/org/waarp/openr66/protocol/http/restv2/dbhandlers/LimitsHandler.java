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
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.LimitDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestLimit;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.utils.RestUtils;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.LIMIT;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.READONLY;
import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.LIMITS_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.requestToObject;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;

/**
 * This is the {@link AbstractRestDbHandler} handling all operation on the
 * host's bandwidth limits.
 */
@Path(LIMITS_HANDLER_URI)
public class LimitsHandler extends AbstractRestDbHandler {

    public LimitsHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to obtain a description of the host's current bandwidth
     * limits.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(LIMIT)
    public void getLimits(HttpRequest request, HttpResponder responder) {

        LimitDAO limitDAO = null;
        try {
            limitDAO = DAO_FACTORY.getLimitDAO();
            if (limitDAO.exist(HOST_ID)) {
                RestLimit restLimit = new RestLimit(limitDAO.select(HOST_ID));
                String responseBody = objectToJson(restLimit);
                responder.sendJson(OK, responseBody);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (limitDAO != null) {
                limitDAO.close();
            }
        }
    }

    /**
     * Method called to initiate the entry for this host in the bandwidth limits
     * database. If the host already has limits set in its configuration,
     * they will be replaced by these new ones.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @POST
    @Consumes(APPLICATION_JSON)
    @RequiredRole(READONLY)
    public void initializeLimits(HttpRequest request, HttpResponder responder) {

        RestLimit restLimit =
                requestToObject(request, RestLimit.class, true);
        LimitDAO limitDAO = null;
        try {
            limitDAO = DAO_FACTORY.getLimitDAO();

            if (!limitDAO.exist(HOST_ID)) {
                limitDAO.insert(restLimit.toLimit());
                configuration.changeNetworkLimit(
                        restLimit.downGlobalLimit, restLimit.upGlobalLimit,
                        restLimit.downSessionLimit, restLimit.upSessionLimit,
                        restLimit.delayLimit);
                String responseBody = objectToJson(restLimit);
                responder.sendJson(CREATED, responseBody);
            } else {
                Error error = ALREADY_EXISTING(HOST_ID);
                Locale lang = getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, error.serialize(lang));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (limitDAO != null) {
                limitDAO.close();
            }
        }
    }

    /**
     * Method called to update this host's bandwidth limits in the database
     * and configuration.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply
     *                  to the request.
     */
    @PUT
    @Consumes(APPLICATION_JSON)
    @RequiredRole(LIMIT)
    public void updateLimits(HttpRequest request, HttpResponder responder) {

        LimitDAO limitDAO = null;
        try {
            limitDAO = DAO_FACTORY.getLimitDAO();

            if (!limitDAO.exist(HOST_ID)) {
                responder.sendStatus(NOT_FOUND);
                return;
            }

            RestLimit partialLimits =
                    requestToObject(request, RestLimit.class, false);

            RestLimit oldLimits = new RestLimit(limitDAO.select(HOST_ID));
            RestLimit newLimits = RestUtils.updateRestObject(oldLimits, partialLimits);

            limitDAO.update(newLimits.toLimit());

            configuration.changeNetworkLimit(
                    newLimits.downGlobalLimit, newLimits.upGlobalLimit,
                    newLimits.downSessionLimit, newLimits.upSessionLimit,
                    newLimits.delayLimit);
            String responseBody = objectToJson(newLimits);
            responder.sendJson(CREATED, responseBody);

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (limitDAO != null) {
                limitDAO.close();
            }
        }
    }

    /**
     * Method called to remove any existing bandwidth limits for this host in
     * the database. Also removes any limits set in the configuration.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @DELETE
    @Consumes(WILDCARD)
    @RequiredRole(LIMIT)
    public void deleteLimits(HttpRequest request, HttpResponder responder) {
        LimitDAO limitDAO = null;
        try {
            limitDAO = DAO_FACTORY.getLimitDAO();

            if (limitDAO.exist(HOST_ID)) {
                limitDAO.delete(limitDAO.select(HOST_ID));
                configuration.changeNetworkLimit(0, 0, 0, 0, 0);
                responder.sendStatus(NO_CONTENT);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (limitDAO != null) {
                limitDAO.close();
            }
        }
    }

    /**
     * Method called to get a list of all allowed HTTP methods on this entry
     * point. The HTTP methods are sent as an array in the reply's headers.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void options(HttpRequest request, HttpResponder responder) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        String allow = RestUtils.getMethodList(this.getClass(), this.crud);
        headers.add("allow", allow);
        responder.sendStatus(OK, headers);
    }
}
