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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.converters.HostConverter;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.utils.JsonUtils;
import org.waarp.openr66.protocol.http.restv2.utils.RestUtils;

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
import java.util.List;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.util.Collections.sort;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.*;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.dao.database.DBHostDAO.*;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOSTS_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;

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
                            @QueryParam(Params.order) @DefaultValue("ascId")
                                        String order_str,
                            @QueryParam(Params.address) String address,
                            @QueryParam(Params.isSSL) String isSSL_str,
                            @QueryParam(Params.isActive) String isActive_str) {

        List<Error> errors = new ArrayList<Error>();

        int limit = 20, offset = 0;
        HostConverter.Order order = HostConverter.Order.ascId;
        try {
            limit = Integer.parseInt(limit_str);
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        }
        try {
            order = HostConverter.Order.valueOf(order_str);
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
                isSSL = RestUtils.stringToBoolean(isSSL_str);
            } catch (ParseException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.isSSL, isSSL_str));
            }
        }
        if (isActive_str != null) {
            try {
                isActive = RestUtils.stringToBoolean(isActive_str);
            } catch (ParseException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.isActive, isActive_str));
            }
        }

        if(!errors.isEmpty()) {
            Locale lang = RestUtils.getRequestLocale(request);
            responder.sendJson(BAD_REQUEST, Error.serializeErrors(errors, lang));
            return;
        }


        List<Filter> filters = new ArrayList<Filter>();
        if (address != null) {
            filters.add(new Filter(ADDRESS_FIELD, "=", address));
        }
        if (isSSL_str != null) {
            filters.add(new Filter(IS_SSL_FIELD, "=", isSSL));
        }
        if (isActive_str != null) {
            filters.add(new Filter(IS_ACTIVE_FIELD, "=", isActive));
        }
        List<Host> hosts;
        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            hosts = hostDAO.find(filters);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }

        int totalResults = hosts.size();
        sort(hosts, order.comparator);

        ArrayNode results = new ArrayNode(JsonNodeFactory.instance);
        for (int i = offset; i < offset + limit && i < hosts.size(); i++) {
            results.add(HostConverter.hostToNode(hosts.get(i)));
        }

        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
        responseObject.put("totalResults", totalResults);
        responseObject.set("results", results);

        String responseText = JsonUtils.nodeToString(responseObject);
        responder.sendJson(OK, responseText);
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

        ObjectNode requestObject = JsonUtils.deserializeRequest(request);
        Host host = HostConverter.nodeToNewHost(requestObject);

        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();

            if (!hostDAO.exist(host.getHostid())) {
                throw new UserErrorException(ALREADY_EXISTING(host.getHostid()));
            }

            hostDAO.insert(host);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }

        ObjectNode responseObject = HostConverter.hostToNode(host);
        String responseText = JsonUtils.nodeToString(responseObject);

        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(CONTENT_TYPE, APPLICATION_JSON);
        headers.add("host-uri", HOSTS_HANDLER_URI + host.getHostid());

        responder.sendString(CREATED, responseText, headers);
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
        String allow = RestUtils.getMethodList(this.getClass(), this.crud);
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}
