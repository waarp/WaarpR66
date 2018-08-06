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
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.Transfer;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;
import org.waarp.openr66.protocol.http.restv2.testdatabases.TransfersDatabase;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.Calendar;
import java.util.List;

/**
 * This is the handler for all request made on the 'transfers' database collection, accessible through the
 * "/v2/transfers" URI.
 */
@Path("/v2/transfers")
public class TransfersHandler extends AbstractHttpHandler {

    /**
     * The method called when a GET request is made on /v2/transfers. If the request is valid, the Http response will
     * contain an array of transfer entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param limit         Maximum number of entries allowed in the response.
     * @param offset        Index of the first accepted entry in the list of all valid answers.
     * @param orderName     The criteria used to sort the entries and the direction of the ordering.
     * @param ruleID        Filter transfers that use this rule.
     * @param partner       Filter transfers that have this partner.
     * @param status        Filter transfers currently in one of these statutes.
     * @param fileName      Filter transfers of a particular file.
     * @param startTrans    Lower bound for the transfers' date.
     * @param stopTrans     Upper bound for the transfers' date.
     */
    @GET
    public void filterTransfer(HttpRequest request, HttpResponder responder,
                               @QueryParam("limit") @DefaultValue("20") int limit,
                               @QueryParam("offset") @DefaultValue("0") int offset,
                               @QueryParam("order") @DefaultValue("+id") String orderName,
                               @QueryParam("ruleID") @DefaultValue("") String ruleID,
                               @QueryParam("partner") @DefaultValue("") String partner,
                               @QueryParam("status") List<Transfer.Status> status,
                               @QueryParam("fileName") @DefaultValue("") String fileName,
                               @QueryParam("startTrans") @DefaultValue("") String startTrans,
                               @QueryParam("stopTrans") @DefaultValue("") String stopTrans) {

        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("limits"));
        } else if (offset < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("offset"));
        } else {
            Calendar start;
            Calendar stop;
            try {
                start = (startTrans.isEmpty() ? null : RestUtils.toCalendar(startTrans));
            } catch (Exception e) {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notADate("startTrans", startTrans));
                return;
            }
            try {
                stop = (stopTrans.isEmpty()) ? null : RestUtils.toCalendar(stopTrans);
            } catch (Exception e) {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notADate("startTrans", startTrans));
                return;
            }
            Transfer.Order order;
            try {
                order = Transfer.Order.fromString(orderName);
            } catch (InstantiationException e) {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.invalidEnum("order", orderName));
                return;
            }

            List<Transfer> resultsList = TransfersDatabase.selectFilter(limit, offset, order,
                    ruleID, partner, status, fileName, start, stop);
            Integer nbResults = resultsList.size();
            String totalResults = "\"totalResults\":" + nbResults.toString();


            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
            try {
                String results = "\"results\":" + mapper.writeValueAsString(resultsList);

                String responseBody = "{" + totalResults + "," + results + "}";
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            } catch (JsonProcessingException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
            }

        }
    }

    /**
     * The method called when a POST request is made on /v2/transfers. If the request is valid, a new transfer entry
     * will be added to the database, and that entry will be returned in the http response. If the request isn't valid
     * the response will contain a '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @POST
    public void createTransfer(HttpRequest request, HttpResponder responder) {

        try {
            Transfer trans = RestUtils.deserializeRequest(request, Transfer.class);
            RestUtils.checkEntry(trans);
            trans.initValues();

            TransfersDatabase.insert(trans);

            String responseBody = RestUtils.toJsonString(trans);
            responder.sendJson(HttpResponseStatus.CREATED, responseBody);
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
     * The method called when an OPTIONS request is made on /v2/transfers. If the request is valid, a response will be
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

