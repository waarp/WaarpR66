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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.http.restv2.RestConstants;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.pojo.UpdatedInfo.TOSUBMIT;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.*;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on
 * the host's transfer database.
 */
@Path(TRANSFER_ID_HANDLER_URI)
public class TransferIdHandler extends AbstractRestDbHandler {

    /** The URI sub-path to restart a transfer. */
    private static final String RESTART_URI = "restart";

    /** The URI sub-path to pause a transfer. */
    private static final String STOP_URI = "stop";

    /** The URI sub-path to cancel a transfer. */
    private static final String CANCEL_URI = "cancel";

    public TransferIdHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to obtain the information on the transfer whose id was
     * given in the request's URI. The requested transfer is sent back in
     * JSON format, unless an unexpected error prevents it or if the request
     * id does not exist.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param uri The requested transfer's id and requested host, separated by
     *            an underscore '_' character.
     * @throws DAOException Thrown if an error occurred when contacting the
     *                      database.
     * @throws JsonProcessingException Thrown if an error occurred during
     *                                 the JSON serialization.
     */
    @GET
    @Consumes(WILDCARD)
    @RequiredRole(READONLY)
    public void getTransfer(HttpRequest request, HttpResponder responder,
                            @PathParam(URI_ID) String uri)
            throws UnsupportedEncodingException {

        String key = URLDecoder.decode(uri, UTF8_CHARSET.name());
        Pattern pattern = Pattern.compile("(-?\\d+)_(.+)");
        Matcher matcher = pattern.matcher(key);
        if (!matcher.find()) {
            responder.sendStatus(NOT_FOUND);
            return;
        }
        String id = matcher.group(1);
        String requested = matcher.group(2);

        TransferDAO transferDAO = null;
        try {
            long transID = Long.parseLong(id);
            transferDAO = RestConstants.factory.getTransferDAO();
            if (!transferDAO.exist(transID)) {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            } else {
                RestTransfer trans = new RestTransfer(transferDAO.select(transID));
                String responseBody = RestUtils.serialize(trans);
                responder.sendJson(HttpResponseStatus.OK, responseBody);
            }
        } catch (NumberFormatException e) {
            responder.sendStatus(NOT_FOUND);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }
    }

    /**
     * Method called to restart a paused transfer.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param uri The requested transfer's id and requested host, separated by
     *            an underscore '_' character.
     * @throws DAOException Thrown if an error occurred when contacting the
     *                      database.
     * @throws JsonProcessingException Thrown if an error occurred during
     *                                 the JSON serialization.
     */
    @Path(RESTART_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(SYSTEM)
    public void restartTransfer(HttpRequest request, HttpResponder responder,
                                @PathParam(URI_ID) String uri)
            throws UnsupportedEncodingException {

        String key = URLDecoder.decode(uri, Charset.forName("UTF-8").name());
        Pattern pattern = Pattern.compile("(-?\\d+)_(.+)");
        Matcher matcher = pattern.matcher(key);
        if (!matcher.find()) {
            responder.sendStatus(NOT_FOUND);
            return;
        }
        String id = matcher.group(1);
        String requested = matcher.group(2);

        TransferDAO transferDAO = null;
        try {
            long transID = Long.parseLong(id);
            transferDAO = RestConstants.factory.getTransferDAO();
            if (!transferDAO.exist(transID)) {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, requested);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.stopTransfer(transfer);
                transfer.setUpdatedInfo(TOSUBMIT);
                transfer.setGlobalStep(transfer.getLastGlobalStep());
                transferDAO.update(transfer);

                ObjectNode response = RestTransferUtils.transferToNode(transfer);
                String responseText = JsonUtils.nodeToString(response);
                responder.sendJson(OK, responseText);
            }
        } catch (NumberFormatException e) {
            responder.sendStatus(NOT_FOUND);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }
    }


    /**
     * Method called to pause a staged or running transfer.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param uri The requested transfer's id and requested host, separated by
     *            an underscore '_' character.
     * @throws DAOException Thrown if an error occurred when contacting the
     *                      database.
     * @throws JsonProcessingException Thrown if an error occurred during
     *                                 the JSON serialization.
     */
    @Path(STOP_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(SYSTEM)
    public void stopTransfer(HttpRequest request, HttpResponder responder,
                             @PathParam(URI_ID) String uri)
            throws UnsupportedEncodingException {

        String key = URLDecoder.decode(uri, Charset.forName("UTF-8").name());
        Pattern pattern = Pattern.compile("(-?\\d+)_(.+)");
        Matcher matcher = pattern.matcher(key);
        if (!matcher.find()) {
            responder.sendStatus(NOT_FOUND);
            return;
        }
        String id = matcher.group(1);
        String requested = matcher.group(2);

        TransferDAO transferDAO = null;
        try {
            long transID = Long.parseLong(id);
            transferDAO = RestConstants.factory.getTransferDAO();
            if (!transferDAO.exist(transID)) {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, requested);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.stopTransfer(transfer);
                transferDAO.update(transfer);

                ObjectNode response = RestTransferUtils.transferToNode(transfer);
                String responseText = JsonUtils.nodeToString(response);
                responder.sendJson(OK, responseText);
            }
        } catch (NumberFormatException e) {
            responder.sendStatus(NOT_FOUND);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
            }
        }
    }

    /**
     * Method called to cancel a staged or running transfer.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to
     *                  the request.
     * @param uri The requested transfer's id and requested host, separated by
     *            an underscore '_' character.
     * @throws DAOException Thrown if an error occurred when contacting the
     *                      database.
     * @throws JsonProcessingException Thrown if an error occurred during
     *                                 the JSON serialization.
     */
    @Path(CANCEL_URI)
    @PUT
    @Consumes(WILDCARD)
    @RequiredRole(SYSTEM)
    public void cancelTransfer(HttpRequest request, HttpResponder responder,
                               @PathParam(URI_ID) String uri)
            throws UnsupportedEncodingException {

        String key = URLDecoder.decode(uri, Charset.forName("UTF-8").name());
        Pattern pattern = Pattern.compile("(-?\\d+)_(.+)");
        Matcher matcher = pattern.matcher(key);
        if (!matcher.find()) {
            responder.sendStatus(NOT_FOUND);
            return;
        }
        String id = matcher.group(1);
        String requested = matcher.group(2);

        TransferDAO transferDAO = null;
        try {
            long transID = Long.parseLong(id);
            transferDAO = RestConstants.factory.getTransferDAO();
            if (!transferDAO.exist(transID)) {
                responder.sendStatus(HttpResponseStatus.NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, requested);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.cancelTransfer(transfer);
                transferDAO.update(transfer);

                ObjectNode response = RestTransferUtils.transferToNode(transfer);
                String responseBody = JsonUtils.nodeToString(response);
                responder.sendJson(OK, responseBody);
            }
        } catch (NumberFormatException e) {
            responder.sendStatus(NOT_FOUND);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (transferDAO != null) {
                transferDAO.close();
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
     * @param uri The requested transfer's id and requested host, separated by
     *            an underscore '_' character.
     */
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void options(HttpRequest request, HttpResponder responder,
                        @PathParam(URI_ID) String uri) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        String allow = RestUtils.getMethodList(this.getClass(), this.crud);
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}
