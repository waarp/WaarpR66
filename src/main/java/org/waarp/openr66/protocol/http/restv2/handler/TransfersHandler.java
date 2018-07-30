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
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.transfers.Transfer;
import org.waarp.openr66.protocol.http.restv2.data.transfers.Transfers;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * This is the handler for all request made on the 'transfers' database collection, accessible through the
 * "/v2/transfers" URI.
 */
@Path("/v2/transfers")
public class TransfersHandler extends AbstractHttpHandler {

    public static final class TransferInitializer {

        public String fileName;

        public String ruleID;

        public String requested;

        public String fileInfo = "";

        public Integer blockSize = 4096;

        public String start;
    }

    /**
     * The method called when a GET request is made on /v2/transfers. If the request is valid, the Http response will
     * contain an array of transfer entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @GET
    public void filterTransfer(HttpRequest request, HttpResponder responder,
                               @QueryParam("limit") @DefaultValue("20") Integer limit,
                               @QueryParam("offset") @DefaultValue("0") Integer offset,
                               @QueryParam("order") @DefaultValue("ascTransferID") Transfer.Order order,
                               @QueryParam("ruleID") @DefaultValue("") String ruleID,
                               @QueryParam("partner") @DefaultValue("") String partner,
                               @QueryParam("status") List<Transfer.Status> status,
                               @QueryParam("fileName") @DefaultValue("") String fileName,
                               @QueryParam("startTrans") @DefaultValue("") String startTrans,
                               @QueryParam("stopTrans") @DefaultValue("") String stopTrans) {

        try {
            Map.Entry<Integer, List<Transfer>> answer = Transfers.filterTransfers(limit, offset, order, ruleID, partner,
                    status, fileName, startTrans, stopTrans);
            List<Transfer> resultsList = answer.getValue();
            Integer nbResults = answer.getKey();
            String totalResults = "\"totalResults\":" + nbResults.toString();

            ObjectMapper mapper = new ObjectMapper();
            String results;
            try {
                results = "\"results\":" + mapper.writeValueAsString(resultsList);
            } catch (JsonProcessingException e) {
                throw OpenR66RestInternalServerException.jsonProcessing();
            }
            String responseBody = "{" + totalResults + "," + results + "}";
            responder.sendJson(HttpResponseStatus.OK, responseBody);

        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
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
            TransferInitializer init = RestUtils.deserializeRequest(request, TransferInitializer.class);
            Calendar start = null;
            if (init.start != null) {
                try {
                    start = RestUtils.toCalendar(init.start);
                } catch (IllegalArgumentException e) {
                    throw OpenR66RestBadRequestException.notADate("start", init.start);
                }
            }

            Transfer newTrans = Transfers.createTransfer(init.fileName, init.ruleID, init.blockSize, init.fileInfo,
                    start, init.requested);

            String responseBody = Transfers.toJsonString(newTrans);
            responder.sendJson(HttpResponseStatus.CREATED, responseBody);
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
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

