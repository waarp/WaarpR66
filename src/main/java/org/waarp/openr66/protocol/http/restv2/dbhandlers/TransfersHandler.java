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
import org.joda.time.DateTime;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.http.restv2.converters.RestTransferUtils;
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
import javax.ws.rs.core.HttpHeaders;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.*;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.dao.database.DBTransferDAO.*;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.TRANSFERS_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on
 * a single entry of the host's transfer database.
 */
@Path(TRANSFERS_HANDLER_URI)
public class TransfersHandler extends AbstractRestDbHandler {

    private static final class Params {
        static final String limit ="limit";
        static final String offset ="offset";
        static final String order ="order";
        static final String ruleID ="ruleID";
        static final String partner ="partner";
        static final String status ="status";
        static final String filename ="filename";
        static final String startTrans ="startTrans";
        static final String stopTrans ="stopTrans";
    }

    public TransfersHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to obtain a list of transfer entry matching the different
     * filters given as parameters of the query. The response is sent as a JSON
     * array containing all the requested entries, unless an unexpected error
     * prevents it or if the request is invalid.
     *
     * @param request    The {@link HttpRequest} made on the resource.
     * @param responder  The {@link HttpResponder} which sends the reply to
     *                   the request.
     * @param limit_str  HTTP query parameter, maximum number of entries allowed
     *                   in the response.
     * @param offset_str HTTP query parameter, index of the first accepted entry
     *                   in the list of all valid answers.
     * @param order_str  HTTP query parameter, the criteria used to sort the
     *                   entries and the way of ordering.
     * @param ruleID     HTTP query parameter, filter transfers that use this rule.
     * @param partner    HTTP query parameter, filter transfers that have this partner.
     * @param status_str HTTP query parameter, filter transfers currently in one
     *                   of these statuses.
     * @param filename   HTTP query parameter, filter transfers of a particular file.
     * @param startTrans HTTP query parameter, lower bound for the transfers'
     *                   starting date (in ISO-8601).
     * @param stopTrans  HTTP query parameter, upper bound for the transfers'
     *                   starting date (in ISO-8601).
     */
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(READONLY)
    public void filterTransfer(HttpRequest request, HttpResponder responder,
                               @QueryParam(Params.limit) @DefaultValue("20")
                                           String limit_str,
                               @QueryParam(Params.offset) @DefaultValue("0")
                                           String offset_str,
                               @QueryParam(Params.order) @DefaultValue("ascId")
                                           String order_str,
                               @QueryParam(Params.ruleID) @DefaultValue("")
                                           String ruleID,
                               @QueryParam(Params.partner) @DefaultValue("")
                                           String partner,
                               @QueryParam(Params.status) @DefaultValue("")
                                           String status_str,
                               @QueryParam(Params.filename) @DefaultValue("")
                                           String filename,
                               @QueryParam(Params.startTrans) @DefaultValue("")
                                           String startTrans,
                               @QueryParam(Params.stopTrans) @DefaultValue("")
                                           String stopTrans) {

        ArrayList<Error> errors = new ArrayList<Error>();

        int limit = 20;
        try {
            limit = Integer.parseInt(limit_str);
        } catch (NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        }
        int offset = 0;
        try {
            offset = Integer.parseInt(offset_str);
        } catch (NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.offset, offset_str));
        }
        RestTransferUtils.Order order = RestTransferUtils.Order.ascId;
        try {
            order = RestTransferUtils.Order.valueOf(order_str);
        } catch (IllegalArgumentException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.order, order_str));
        }

        List<Filter> filters = new ArrayList<Filter>();
        if (!startTrans.isEmpty()) {
            try {
                DateTime start = DateTime.parse(startTrans);
                filters.add(new Filter(TRANSFER_START_FIELD, ">=", start.getMillis()));
            } catch (IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.startTrans, startTrans));
            }
        }
        if (!stopTrans.isEmpty()) {
            try {
                DateTime stop = DateTime.parse(stopTrans);
                filters.add(new Filter(TRANSFER_START_FIELD, "<=", stop.getMillis()));
            } catch (IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.stopTrans, stopTrans));
            }
        }
        if (!ruleID.isEmpty()) {
            filters.add(new Filter(ID_RULE_FIELD, "=", ruleID));
        }
        if (!partner.isEmpty()) {
            filters.add(new Filter(REQUESTED_FIELD, "=", partner));
        }
        if (!filename.isEmpty()) {
            filters.add(new Filter(FILENAME_FIELD, "=", filename));
        }
        if (!status_str.isEmpty()) {
            try {
                int status_nbr = AbstractDbData.UpdatedInfo.valueOf(status_str).ordinal();
                filters.add(new Filter(UPDATED_INFO_FIELD, "=", status_nbr));
            } catch (IllegalArgumentException e) {
                errors.add(ILLEGAL_PARAMETER_VALUE(Params.status, status_str));
            }
        }

        if(!errors.isEmpty()) {
            Locale lang = RestUtils.getRequestLocale(request);
            responder.sendJson(BAD_REQUEST, Error.serializeErrors(errors, lang));
            return;
        }

        TransferDAO transferDAO = null;
        List<Transfer> transferList;
        try {
            transferDAO = DAO_FACTORY.getTransferDAO();
            transferList = transferDAO.find(filters, order.column, order.ascend,
                    limit, offset);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }

        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode resultList = responseObject.putArray("results");
        for (Transfer transfer : transferList) {
            resultList.add(RestTransferUtils.transferToNode(transfer));
        }
        responseObject.put("totalResults", transferList.size());
        String responseText = JsonUtils.nodeToString(responseObject);
        responder.sendJson(OK, responseText);
    }

    /**
     * Method called to create a new transfer on the server. The reply will
     * contain the created entry in JSON format, unless an unexpected error
     * prevents it or if the request is invalid.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to the request.
     */
    @POST
    @Consumes(APPLICATION_JSON)
    @RequiredRole(TRANSFER)
    public void createTransfer(HttpRequest request, HttpResponder responder) {

        ObjectNode requestObject = JsonUtils.deserializeRequest(request);
        Transfer transfer = RestTransferUtils.nodeToNewTransfer(requestObject);

        TransferDAO transferDAO = null;
        try {
            transferDAO = DAO_FACTORY.getTransferDAO();
            transferDAO.insert(transfer);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }

        ObjectNode responseObject = RestTransferUtils.transferToNode(transfer);
        String responseText = JsonUtils.nodeToString(responseObject);
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
        headers.add("transfer-uri", TRANSFERS_HANDLER_URI + transfer.getId() +
                "_" + transfer.getRequested());

        responder.sendString(CREATED, responseText, headers);
    }

    /**
     * Method called to get a list of all allowed HTTP methods on this entry
     * point. The HTTP methods are sent as an array in the reply's headers.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply
     *                  to the request.
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

