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
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestHostConfig;
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
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.CONFIGADMIN;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.READONLY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.CONFIG_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.requestToObject;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on the
 * host's configuration.
 */
@Path(CONFIG_HANDLER_URI)
public class HostConfigHandler extends AbstractRestDbHandler {

    public HostConfigHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to retrieve a host's configuration entry in the database.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(READONLY)
    public void getConfig(HttpRequest request, HttpResponder responder) {

        BusinessDAO businessDAO = null;
        try {
            businessDAO = DAO_FACTORY.getBusinessDAO();
            Business business = businessDAO.select(HOST_ID);
            if (business != null) {
                RestHostConfig config = new RestHostConfig(business);
                String responseBody = objectToJson(config);
                responder.sendJson(OK, responseBody);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (businessDAO != null) {
                businessDAO.close();
            }
        }
    }

    /**
     * Method called to initialize a host's configuration database entry if none
     * already exists.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @POST
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(CONFIGADMIN)
    public void initializeConfig(HttpRequest request, HttpResponder responder) {

        BusinessDAO businessDAO = null;
        try {
            RestHostConfig config =
                    requestToObject(request, RestHostConfig.class, true);

            businessDAO = DAO_FACTORY.getBusinessDAO();

            if (!businessDAO.exist(HOST_ID)) {
                businessDAO.insert(config.toBusiness());
                String responseBody = objectToJson(config);
                responder.sendJson(CREATED, responseBody);
            } else {
                Locale lang = getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, ALREADY_EXISTING(HOST_ID).serialize(lang));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (businessDAO != null) {
                businessDAO.close();
            }
        }
    }

    /**
     * Method called to update a host's configuration in the database if it exists.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @PUT
    @Consumes(APPLICATION_JSON)
    @RequiredRole(CONFIGADMIN)
    public void updateConfig(HttpRequest request, HttpResponder responder) {

        BusinessDAO businessDAO = null;

        try {
            businessDAO = DAO_FACTORY.getBusinessDAO();

            if (!businessDAO.exist(HOST_ID)) {
                responder.sendStatus(NOT_FOUND);
            }

            RestHostConfig partialConfig =
                    requestToObject(request, RestHostConfig.class, false);
            RestHostConfig oldConfig =
                    new RestHostConfig(businessDAO.select(HOST_ID));
            RestHostConfig newConfig =
                    RestUtils.updateRestObject(oldConfig, partialConfig);


            businessDAO.update(newConfig.toBusiness());
            String responseBody = objectToJson(newConfig);
            responder.sendJson(CREATED, responseBody);

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (businessDAO != null) {
                businessDAO.close();
            }
        }
    }

    /**
     * Method called to delete a host's configuration entry in the database.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @DELETE
    @Consumes(WILDCARD)
    @RequiredRole(CONFIGADMIN)
    public void deleteConfig(HttpRequest request, HttpResponder responder) {

        BusinessDAO businessDAO = null;
        try {
            businessDAO = DAO_FACTORY.getBusinessDAO();
            if (businessDAO.exist(HOST_ID)) {
                businessDAO.delete(businessDAO.select(HOST_ID));
                responder.sendStatus(NO_CONTENT);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (businessDAO != null) {
                businessDAO.close();
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
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}

