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
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestHost;
import org.waarp.openr66.protocol.http.restv2.errors.Error;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.Collections.sort;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.HOST;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.READONLY;
import static org.waarp.openr66.dao.database.DBHostDAO.ADDRESS_FIELD;
import static org.waarp.openr66.dao.database.DBHostDAO.IS_ACTIVE_FIELD;
import static org.waarp.openr66.dao.database.DBHostDAO.IS_SSL_FIELD;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOSTS_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.data.RestHost.Order;
import static org.waarp.openr66.protocol.http.restv2.errors.Error.serializeErrors;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ILLEGAL_PARAMETER_VALUE;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.requestToObject;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getMethodList;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.stringToBoolean;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on
 * the host's partner database.
 */
@Path(HOSTS_HANDLER_URI)
public class HostsHandler extends AbstractRestDbHandler {

    private static final class Params {
        static final String limit = "limit";
        static final String offset = "offset";
        static final String order = "order";
        static final String address = "address";
        static final String isSSL = "isSSL";
        static final String isActive = "isActive";
    }

    public HostsHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to get a list of host entries from the server's database,
     * with optional filters applied.
     *
     * @param request    The {@link HttpRequest} made on the resource.
     * @param responder  The {@link HttpResponder} which sends the reply to
     *                   the request.
     * @param limit_str  HTTP query parameter, maximum number of entries
     *                   allowed in the response.
     * @param offset_str HTTP query parameter, index of the first accepted
     *                   entry in the list of all valid answers.
     * @param order_str  HTTP query parameter, the criteria used to sort
     *                   the entries and the way of ordering.
     * @param address    HTTP query parameter, filter only hosts with this address.
     * @param isSSL_str  HTTP query parameter, filter only hosts that use SSL,
     *                     or those that don't. Leave empty
     *                     to get both.
     * @param isActive_str HTTP query parameter, filter hosts that are active,
     *                     or those that aren't. Leave empty
     *                     to get both.
     */
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(READONLY)
    public void filterHosts(HttpRequest request, HttpResponder responder,
                            @QueryParam(Params.limit) @DefaultValue("20")
                                        String limit_str,
                            @QueryParam(Params.offset) @DefaultValue("0")
                                        String offset_str,
                            @QueryParam(Params.order) @DefaultValue("ascHostID")
                                        String order_str,
                            @QueryParam(Params.address) @DefaultValue("")
                                        String address,
                            @QueryParam(Params.isSSL) @DefaultValue("")
                                        String isSSL_str,
                            @QueryParam(Params.isActive) @DefaultValue("")
                                        String isActive_str) {

        List<Error> errors = new ArrayList<Error>();

        int limit = 20, offset = 0;
        Order order = Order.ascHostID;
        try {
            limit = Integer.parseInt(limit_str);
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        }
        try {
            order = Order.valueOf(order_str);
        } catch (IllegalArgumentException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.order, order_str));
        }
        try {
            offset = Integer.parseInt(offset_str);
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.offset, offset_str));
        }

        if (limit < 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        } else if (offset < 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.offset, offset_str));
        }

        boolean isSSL = false, isActive = false;
        if (isSSL_str != null) {
            try {
                isSSL = stringToBoolean(isSSL_str);
            } catch (ParseException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.isSSL, isSSL_str));
            }
        }
        if (isActive_str != null) {
            try {
                isActive = stringToBoolean(isActive_str);
            } catch (ParseException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.isActive, isActive_str));
            }
        }

        if(!errors.isEmpty()) {
            Locale lang = getRequestLocale(request);
            responder.sendJson(BAD_REQUEST, serializeErrors(errors, lang));
            return;
        }


        List<Filter> filters = new ArrayList<Filter>();
        if (!address.isEmpty()) {
            filters.add(new Filter(ADDRESS_FIELD, "=", address));
        }
        if (isSSL_str != null) {
            filters.add(new Filter(IS_SSL_FIELD, "=", isSSL));
        }
        if (isActive_str != null) {
            filters.add(new Filter(IS_ACTIVE_FIELD, "=", isActive));
        }
        List<RestHost> answers;
        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            if (filters.isEmpty()) {
                answers = RestHost.toRestList(hostDAO.getAll());
            } else {
                answers = RestHost.toRestList(hostDAO.find(filters));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }
        int nbResults = answers.size();
        HashMap<String, Object> responseBody = new HashMap<String, Object>();
        responseBody.put("totalResults", nbResults);

        sort(answers, order.comparator);
        List<RestHost> orderedAnswers = new ArrayList<RestHost>();
        for (int i = offset; i < offset + limit && i < answers.size(); i++) {
            orderedAnswers.add(answers.get(i));
        }
        responseBody.put("results", orderedAnswers);
        responder.sendJson(OK, objectToJson(responseBody));
    }

    /**
     * Method called to add a new host authentication entry to the server database.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     */
    @POST
    @Consumes(APPLICATION_JSON)
    @RequiredRole(HOST)
    public void addHost(HttpRequest request, HttpResponder responder) {

        RestHost restHost = requestToObject(request, RestHost.class, true);

        HostDAO hostDAO = null;
        try {
            restHost.encryptPassword();
            hostDAO = DAO_FACTORY.getHostDAO();

            if (!hostDAO.exist(restHost.hostID)) {
                hostDAO.insert(restHost.toHost());
                String responseBody = objectToJson(restHost);
                DefaultHttpHeaders headers = new DefaultHttpHeaders();
                headers.add(CONTENT_TYPE, APPLICATION_JSON);
                headers.add("host-uri", HOSTS_HANDLER_URI + restHost.hostID);
                responder.sendString(CREATED, responseBody, headers);
            } else {
                Error error = ALREADY_EXISTING(restHost.hostID);
                Locale lang = getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, error.serialize(lang));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }
    }

    /**
     * Method called to get a list of all allowed HTTP methods on this entry point.
     * The HTTP methods are sent as an array in the reply's headers.
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
        String allow = getMethodList(this.getClass(), this.crud);
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}
