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
import org.testcontainers.shaded.org.apache.commons.lang.math.NumberUtils;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.database.DBTransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestTransfer;
import org.waarp.openr66.protocol.http.restv2.data.RestTransferInitializer;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestResponse;
import org.waarp.openr66.protocol.http.restv2.errors.InternalErrorResponse;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;

import javax.swing.*;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * This is the handler for all request made on the 'transfers' database collection, accessible through the
 * "/v2/transfers" URI.
 */
@Path("/v2/transfers")
public class TransfersHandler extends AbstractRestHttpHandler {

    public TransfersHandler() {
        super(RoleDefault.ROLE.TRANSFER);
    }

    private static final WaarpLogger logger = WaarpLoggerFactory.getInstance(TransferHandler.class);

    /**
     * The method called when a GET request is made on /v2/transfers. If the request is valid, the Http response will
     * contain an array of transfer entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param limitStr      Maximum number of entries allowed in the response.
     * @param offsetStr     Index of the first accepted entry in the list of all valid answers.
     * @param orderName     The criteria used to sort the entries and the direction of the ordering.
     * @param ruleID        Filter transfers that use this rule.
     * @param partner       Filter transfers that have this partner.
     * @param statuses      Filter transfers currently in one of these statutes.
     * @param fileName      Filter transfers of a particular file.
     * @param startTrans    Lower bound for the transfers' date.
     * @param stopTrans     Upper bound for the transfers' date.
     */
    @GET
    public void filterTransfer(HttpRequest request, HttpResponder responder,
                               @QueryParam("limit") @DefaultValue("20") String limitStr,
                               @QueryParam("offset") @DefaultValue("0") String offsetStr,
                               @QueryParam("order") @DefaultValue("+id") String orderName,
                               @QueryParam("ruleID") @DefaultValue("") String ruleID,
                               @QueryParam("partner") @DefaultValue("") String partner,
                               @QueryParam("status") List<RestTransfer.Status> statuses,
                               @QueryParam("fileName") @DefaultValue("") String fileName,
                               @QueryParam("startTrans") @DefaultValue("") String startTrans,
                               @QueryParam("stopTrans") @DefaultValue("") String stopTrans) {

        BadRequestResponse badResponse = new BadRequestResponse();

        int limit = -1, offset = -1;
        if (NumberUtils.isDigits(limitStr)) {
            limit = Integer.parseInt(limitStr);
        } else {
            badResponse.illegalParameterValue("limit");
        }
        if (NumberUtils.isDigits(offsetStr)) {
            offset = Integer.parseInt(offsetStr);
        } else {
            badResponse.illegalParameterValue("offset");
        }

        Calendar start = null;
        Calendar stop = null;
        try {
            start = (startTrans.isEmpty() ? null : RestUtils.toCalendar(startTrans));
        } catch (IllegalArgumentException e) {
            badResponse.illegalParameterValue("startTrans");
        }
        try {
            stop = (stopTrans.isEmpty()) ? null : RestUtils.toCalendar(stopTrans);
        } catch (IllegalArgumentException e) {
            badResponse.illegalParameterValue("stopTrans");
        }
        RestTransfer.Order order = null;
        try {
            order = RestTransfer.Order.fromString(orderName);
        } catch (InstantiationException e) {
            badResponse.illegalParameterValue("order");
        }

        if(badResponse.isEmpty()) {
            List<Filter> filters = new ArrayList<Filter>();
            filters.add(new Filter(DBTransferDAO.ID_RULE_FIELD, "=", ruleID));
            filters.add(new Filter(DBTransferDAO.REQUESTED_FIELD, "=", partner));
            filters.add(new Filter(DBTransferDAO.FILENAME_FIELD, "=", fileName));
            filters.add(new Filter(DBTransferDAO.TRANSFER_START_FIELD, ">=", start));
            filters.add(new Filter(DBTransferDAO.TRANSFER_START_FIELD, "<=", stop));
            for (RestTransfer.Status status : statuses) {
                filters.add(new Filter(DBTransferDAO.UPDATED_INFO_FIELD, "=", status.name()));
            }

            try {
                TransferDAO transferDAO = RestUtils.factory.getTransferDAO();
                List<RestTransfer> resultsList = RestTransfer.toRestList(transferDAO.find(filters));

                Integer nbResults = resultsList.size();
                String totalResults = "\"totalResults\":" + nbResults.toString();

                Collections.sort(resultsList, order.comparator);
                List<RestTransfer> orderedResults = new ArrayList<RestTransfer>();
                for (int i = offset; i < offset + limit && i < resultsList.size(); i++) {
                    orderedResults.add(resultsList.get(i));
                }

                String results = "\"results\":" + RestUtils.toJsonString(orderedResults);

                String responseBody = "{" + totalResults + "," + results + "}";
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            } catch (JsonProcessingException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.jsonProcessingError().toJson());
            } catch (DAOException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.databaseError().toJson());
            }
        }
        else {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, badResponse.toJson());
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
            RestTransferInitializer init = RestUtils.deserializeRequest(request, RestTransferInitializer.class);
            RestUtils.checkEntry(init);

            TransferDAO transferDAO = RestUtils.factory.getTransferDAO();
            Transfer trans = init.toTransfer();
            transferDAO.insert(trans);

            String responseBody = RestUtils.toJsonString(new RestTransfer(trans));
            responder.sendJson(HttpResponseStatus.CREATED, responseBody);
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.toJson());
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toJson());
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.jsonProcessingError().toJson());
        } catch (DAOException e) {
            e.printStackTrace();
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    InternalErrorResponse.databaseError().toJson());
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

