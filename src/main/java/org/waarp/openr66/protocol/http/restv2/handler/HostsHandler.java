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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.database.DBHostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestHost;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the handler for all request made on the 'hosts' database, accessible through the "/v2/hosts" URI.
 */
@Path("/v2/hosts")
public class HostsHandler extends AbstractHttpHandler {

    /**
     * The method called when a GET request is made on /v2/hosts. If the request is valid, the Http response will
     * contain an array of host entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param request     The Http request made on the resource.
     * @param responder   The Http responder, Http response are given to it in order to be sent back.
     * @param limitStr    Maximum number of entries allowed in the response.
     * @param offsetStr   Index of the first accepted entry in the list of all valid answers.
     * @param orderStr    The criteria used to sort the entries and the direction of the ordering.
     * @param address     Filter hosts with this address.
     * @param isSSLStr    If true filter hosts that use SSL, if false those that don't. Filter both is null.
     * @param isActiveStr If true filter hosts that are active, if false those that aren't. Filter both is null.
     */
    @GET
    public void filterHosts(HttpRequest request, HttpResponder responder,
                            @QueryParam("limit") @DefaultValue("20") String limitStr,
                            @QueryParam("offset") @DefaultValue("0") String offsetStr,
                            @QueryParam("order") @DefaultValue("+id") String orderStr,
                            @QueryParam("address") String address,
                            @QueryParam("isSSL") String isSSLStr,
                            @QueryParam("isActive") String isActiveStr) {

        int limit, offset;
        RestHost.Order order;
        Boolean isSSL, isActive;
        try {
            limit = Integer.parseInt(limitStr);
            order = RestHost.Order.fromString(orderStr);
            isSSL = RestUtils.toBoolean(isSSLStr);
        } catch(NumberFormatException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notANumber(limitStr));
            return;
        } catch (InstantiationException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.invalidEnum("order", orderStr));
            return;
        } catch (IOException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notABoolean(isSSLStr));
            return;
        }
        try {
            offset = Integer.parseInt(offsetStr);
            isActive = RestUtils.toBoolean(isActiveStr);
        } catch(NumberFormatException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notANumber(offsetStr));
            return;
        } catch (IOException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notABoolean(isActiveStr));
            return;
        }

        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("limit"));
        } else if (offset < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("offset"));
        } else {
            try {
                HostDAO dao = RestUtils.factory.getHostDAO();
                List<Filter> filters = new ArrayList<Filter>();
                if(address != null)  filters.add(new Filter(DBHostDAO.ADDRESS_FIELD, "=", address));
                if(isSSL != null)    filters.add(new Filter(DBHostDAO.IS_SSL_FIELD, "=", isSSL));
                if(isActive != null) filters.add(new Filter(DBHostDAO.IS_ACTIVE_FIELD, "=", isActive));
                List<RestHost> answers;
                if(filters.isEmpty()) {
                    answers = RestHost.toRestList(dao.getAll());
                } else {
                    answers = RestHost.toRestList(dao.find(filters));
                }
                Integer nbResults = answers.size();
                String totalResults = "\"totalResults\":" + nbResults.toString();

                Collections.sort(answers, order.comparator);
                List<RestHost> orderedAnswers = new ArrayList<RestHost>();
                for(int i=offset; i<offset+limit && i<answers.size(); i++) {
                    orderedAnswers.add(answers.get(i));
                }

                ObjectMapper mapper = new ObjectMapper();
                String results = "\"results\":" + mapper.writeValueAsString(orderedAnswers);

                String responseBody = "{" + totalResults + "," + results + "}";
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            } catch (DAOException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e));
            } catch (JsonProcessingException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
            }
        }
    }

    /**
     * The method called when a POST request is made on /v2/hosts. If the request is valid and the id does not exists
     * in the database, the new host entry in the request body will be added to the database and sent back in the Http
     * response. If the id does already exist, or if the request is invalid, a '400 - Bad request' error will be sent
     * ²instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @POST
    public void addHost(HttpRequest request, HttpResponder responder) {
        try {
            RestHost restHost = RestUtils.deserializeRequest(request, RestHost.class);
            restHost.defaultValues();
            RestUtils.checkEntry(restHost);

            HostDAO dao = RestUtils.factory.getHostDAO();

            if (!dao.exist(restHost.hostID)) {
                dao.insert(restHost.toHost());
                String responseBody = RestUtils.toJsonString(restHost);
                responder.sendJson(HttpResponseStatus.CREATED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.alreadyExisting("restHost", restHost.hostID));
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        } catch (OpenR66RestInvalidEntryException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/hosts. If the request is valid, a response will be
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
