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
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.http.restv2.converters.TransferConverter;
import org.waarp.openr66.protocol.http.restv2.utils.JsonUtils;
import org.waarp.openr66.protocol.localhandler.ServerActions;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.pojo.UpdatedInfo.TOSUBMIT;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.*;

/**
 * This is the {@link AbstractRestDbHandler} handling all requests made on the
 * single transfer REST entry point.
 */
@Path(TRANSFER_ID_HANDLER_URI)
public class TransferIdHandler extends AbstractRestDbHandler {

    /**
     * Initializes the handler with the given CRUD mask.
     *
     * @param crud the CRUD mask for this handler
     */
    public TransferIdHandler(byte crud) {
        super(crud);
    }

    /**
     * Method called to obtain the information on the transfer whose id was
     * given in the request's URI. The requested transfer is sent back in
     * JSON format, unless an unexpected error prevents it or if the request
     * id does not exist.
     * <p>
     * **NOTE:** The {@code uri} parameter refers to the concatenation of
     * the transfer's id, and the name of the host to which the transfer was
     * requested, separated by an underscore character.
     *
     * @param request   the HttpRequest made to the resource
     * @param responder the HttpResponder which sends the reply to the request
     * @param uri       the transfer's unique identifier
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
            transferDAO = DAO_FACTORY.getTransferDAO();
            if (!transferDAO.exist(transID, SERVER_NAME, requested, SERVER_NAME)) {
                responder.sendStatus(NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, SERVER_NAME, requested, SERVER_NAME);
                ObjectNode response = TransferConverter.transferToNode(transfer);
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
     * Method called to restart a paused transfer.
     * <p>
     * **NOTE:** The {@code uri} parameter refers to the concatenation of
     * the transfer's id, and the name of the host to which the transfer was
     * requested, separated by an underscore character.
     *
     * @param request   the HttpRequest made to the resource
     * @param responder the HttpResponder which sends the reply to the request
     * @param uri       the transfer's unique identifier
     */
    @Path(TransferCommandsURI.RESTART_URI)
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
            transferDAO = DAO_FACTORY.getTransferDAO();
            if (!transferDAO.exist(transID, SERVER_NAME, requested, SERVER_NAME)) {
                responder.sendStatus(NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, SERVER_NAME, requested, SERVER_NAME);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.stopTransfer(transfer);
                transfer.setUpdatedInfo(TOSUBMIT);
                transfer.setGlobalStep(transfer.getLastGlobalStep());
                transferDAO.update(transfer);

                ObjectNode response = TransferConverter.transferToNode(transfer);
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
     * <p>
     * **NOTE:** The {@code uri} parameter refers to the concatenation of
     * the transfer's id, and the name of the host to which the transfer was
     * requested, separated by an underscore character.
     *
     * @param request   the HttpRequest made to the resource
     * @param responder the HttpResponder which sends the reply to the request
     * @param uri       the transfer's unique identifier
     */
    @Path(TransferCommandsURI.STOP_URI)
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
            transferDAO = DAO_FACTORY.getTransferDAO();
            if (!transferDAO.exist(transID, SERVER_NAME, requested, SERVER_NAME)) {
                responder.sendStatus(NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, SERVER_NAME, requested, SERVER_NAME);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.stopTransfer(transfer);
                transferDAO.update(transfer);

                ObjectNode response = TransferConverter.transferToNode(transfer);
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
     * <p>
     * **NOTE:** The {@code uri} parameter refers to the concatenation of
     * the transfer's id, and the name of the host to which the transfer was
     * requested, separated by an underscore character.
     *
     * @param request   the HttpRequest made to the resource
     * @param responder the HttpResponder which sends the reply to the request
     * @param uri       the transfer's unique identifier
     */
    @Path(TransferCommandsURI.CANCEL_URI)
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
            transferDAO = DAO_FACTORY.getTransferDAO();
            if (!transferDAO.exist(transID, SERVER_NAME, requested, SERVER_NAME)) {
                responder.sendStatus(NOT_FOUND);
            } else {
                Transfer transfer = transferDAO.select(transID, SERVER_NAME, requested, SERVER_NAME);
                ServerActions actions = new ServerActions();
                actions.newSession();
                actions.cancelTransfer(transfer);
                transferDAO.update(transfer);

                ObjectNode response = TransferConverter.transferToNode(transfer);
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
     * <p>
     * **NOTE:** The {@code uri} parameter refers to the concatenation of
     * the transfer's id, and the name of the host to which the transfer was
     * requested, separated by an underscore character.
     *
     * @param request   the HttpRequest made to the resource
     * @param responder the HttpResponder which sends the reply to the request
     * @param uri       the transfer's unique identifier
     */
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void options(HttpRequest request, HttpResponder responder,
                        @PathParam(URI_ID) String uri) {
        HttpHeaders allow = new DefaultHttpHeaders();
        allow.add(ALLOW, HttpMethod.OPTIONS);
        responder.sendStatus(OK, allow);
    }

    @Path("{ep}")
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void sub_options(HttpRequest request, HttpResponder responder,
                            @PathParam(URI_ID) String uri, @PathParam("ep") String ep) {

        HttpHeaders allow = new DefaultHttpHeaders();
        List<HttpMethod> methods = new ArrayList<HttpMethod>();

        if (ep.equals(TransferCommandsURI.RESTART_URI)) {
            methods.add(HttpMethod.PUT);
        } else if (ep.equals(TransferCommandsURI.STOP_URI)) {
            methods.add(HttpMethod.PUT);
        } else if (ep.equals(TransferCommandsURI.CANCEL_URI)) {
            methods.add(HttpMethod.PUT);
        } else {
            responder.sendStatus(NOT_FOUND);
            return;
        }
        methods.add(HttpMethod.OPTIONS);
        allow.add(ALLOW, methods);
        responder.sendStatus(OK, allow);
    }
}
