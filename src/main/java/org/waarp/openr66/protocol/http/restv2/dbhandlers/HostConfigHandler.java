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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.waarp.openr66.protocol.http.restv2.converters.HostConfigConverter;
import org.waarp.openr66.protocol.http.restv2.utils.JsonUtils;
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

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.*;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ALREADY_EXISTING;

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
            Business business = businessDAO.select(SERVER_NAME);
            if (business != null) {
                ObjectNode responseObject = HostConfigConverter.businessToNode(business);
                String responseText = JsonUtils.nodeToString(responseObject);
                responder.sendJson(OK, responseText);
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
            businessDAO = DAO_FACTORY.getBusinessDAO();

            if (!businessDAO.exist(SERVER_NAME)) {
                ObjectNode requestObject = JsonUtils.deserializeRequest(request);
                Business config = HostConfigConverter.nodeToNewBusiness(requestObject);
                businessDAO.insert(config);

                ObjectNode responseObject = HostConfigConverter.businessToNode(config);
                String responseText = JsonUtils.nodeToString(responseObject);
                responder.sendJson(CREATED, responseText);
            } else {
                Locale lang = RestUtils.getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, ALREADY_EXISTING(SERVER_NAME).serialize(lang));
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

            if (!businessDAO.exist(SERVER_NAME)) {
                responder.sendStatus(NOT_FOUND);
            }

            ObjectNode requestObject = JsonUtils.deserializeRequest(request);
            Business oldConfig = businessDAO.select(SERVER_NAME);
            Business newConfig = HostConfigConverter.nodeToUpdatedBusiness(requestObject, oldConfig);
            businessDAO.update(newConfig);

            ObjectNode responseObject = HostConfigConverter.businessToNode(newConfig);
            String responseText = JsonUtils.nodeToString(responseObject);
            responder.sendJson(CREATED, responseText);
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
            if (businessDAO.exist(SERVER_NAME)) {
                businessDAO.delete(businessDAO.select(SERVER_NAME));
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

