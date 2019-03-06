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
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestRule;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.RULE;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.RULE_ID_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.URI_ID;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.FIELD_NOT_ALLOWED;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.requestToObject;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getMethodList;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.updateRestObject;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on a
 * single entry of the host's transfer rule database.
 */
@Path(RULE_ID_HANDLER_URI)
public class RuleIdHandler extends AbstractRestDbHandler {

    public RuleIdHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to retrieve a transfer rule with the id given in the
     * request URI.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param id The requested transfer's id, this refers to the {id} parameter
     *           in the URI of the request.
     */
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void getRule(HttpRequest request, HttpResponder responder,
                        @PathParam(URI_ID) String id) {

        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            if (ruleDAO.exist(id)) {
                RestRule restRule = new RestRule(ruleDAO.select(id));
                String responseBody = objectToJson(restRule);
                responder.sendJson(OK, responseBody);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }
    }

    /**
     * Method called to update a transfer rule.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param id The requested transfer's id, this refers to the {id} parameter
     *           in the URI of the request.
     */
    @PUT
    @Consumes(APPLICATION_JSON)
    @RequiredRole(RULE)
    public void updateRule(HttpRequest request, HttpResponder responder,
                           @PathParam(URI_ID) String id) {

        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            if (!ruleDAO.exist(id)) {
                responder.sendStatus(NOT_FOUND);
            }

            Locale lang = getRequestLocale(request);
            RestRule partialRule =
                    requestToObject(request, RestRule.class, false);
            if (partialRule.ruleID != null && !partialRule.ruleID.equals(id)) {
                responder.sendJson(BAD_REQUEST,
                        FIELD_NOT_ALLOWED("ruleID").serialize(lang));
                return;
            }

            RestRule oldRule = new RestRule(ruleDAO.select(id));
            RestRule newRule = updateRestObject(oldRule, partialRule);

            ruleDAO.update(newRule.toRule());
            String responseBody = objectToJson(newRule);
            responder.sendJson(ACCEPTED, responseBody);

        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }
    }

    /**
     * Method called to delete a transfer rule from the database.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param id The requested transfer's id, this refers to the {id} parameter
     *           in the URI of the request.
     */
    @DELETE
    @Consumes(WILDCARD)
    @RequiredRole(RULE)
    public void deleteRule(HttpRequest request, HttpResponder responder,
                           @PathParam(URI_ID) String id) {

        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            if (ruleDAO.exist(id)) {
                ruleDAO.delete(ruleDAO.select(id));
                responder.sendStatus(NO_CONTENT);
            } else {
                responder.sendStatus(NOT_FOUND);
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
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
     * @param id The requested transfer's id, this refers to the {id} parameter
     *           in the URI of the request.
     */
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void options(HttpRequest request, HttpResponder responder,
                        @PathParam(URI_ID) String id) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        String allow = getMethodList(this.getClass(), this.crud);
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}