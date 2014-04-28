/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.localhandler.rest.handler;


import java.util.Date;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler.COMMAND_TYPE;
import org.waarp.gateway.kernel.rest.HttpRestHandler.METHOD;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.InformationJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.StopOrCancelJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.RestartTransferJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket;
import org.waarp.openr66.protocol.localhandler.rest.HttpRestR66Handler;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transfer Http REST interface: http://host/control?... + 
 * InformationJsonPacket (should be on Transfer only) RestartTransferJsonPacket StopOrCancelJsonPacket TransferRequestJsonPacket 
 * as GET PUT PUT POST
 * @author "Frederic Bregier"
 *
 */
public class HttpRestTransferR66Handler extends HttpRestAbstractR66Handler {
	
	public static final String BASEURI = "control";
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestTransferR66Handler.class);
   
	/**
	 * @param path
	 * @param method
	 */
	public HttpRestTransferR66Handler() {
		super(BASEURI, METHOD.GET, METHOD.PUT, METHOD.POST);
	}
	
	@Override
	public void endParsingRequest(HttpRestHandler handler, RestArgument arguments, RestArgument result, Object body)
			throws HttpIncorrectRequestException, HttpInvalidAuthenticationException {
		logger.debug("debug: {} ### {}",arguments,result);
		if (body != null) {
			logger.debug("Obj: {}", body);
		}
		handler.setWillClose(false);
		ServerActions serverHandler = ((HttpRestR66Handler) handler).serverHandler;
		R66Session session = serverHandler.getSession();
		// now action according to body
		JsonPacket json = (JsonPacket) body;
		if (json == null) {
			result.addItem(JSON_DETAIL, "not enough information");
			setError(handler, result, ErrorCode.CommandNotFound);
			return;
		}
		METHOD method = arguments.getMethod();
		try {
			if (json instanceof InformationJsonPacket && method == METHOD.GET) {//
				InformationJsonPacket node = (InformationJsonPacket) json;
				ValidPacket validPacket = serverHandler.information(node.isIdRequest(), node.getId(), node.isTo(), node.getRequest(), node.getRulename(), node.getFilename(), true);
				if (validPacket != null) {
					ObjectNode resp = JsonHandler.getFromString(validPacket.getSheader());
					handler.setStatus(HttpResponseStatus.OK);
					result.addItem(JSON_MESSAGE, ErrorCode.CompleteOk.mesg);
					result.addItem(JSON_MESSAGECODE, ""+ErrorCode.CompleteOk.code);
					result.getAnswer().put(JSON_RESULT, resp);
				} else {
					result.addItem(JSON_DETAIL, "Error during information request");
					setError(handler, result, ErrorCode.TransferError);
				}
			} else if (json instanceof RestartTransferJsonPacket && method == METHOD.PUT) {//
				RestartTransferJsonPacket node = (RestartTransferJsonPacket) json;
				R66Result r66result = serverHandler.requestRestart(node.getRequested(), node.getRequester(), node.getSpecialid(), node.getRestarttime());
				if (serverHandler.isCodeValid(r66result.code)) {
					setOk(handler, result, node, r66result.code);
				} else {
					result.addItem(JSON_DETAIL, "Restart Transfer in error");
					setError(handler, result, node, ErrorCode.TransferError);
				}
			} else if (json instanceof StopOrCancelJsonPacket && method == METHOD.PUT) {//
				StopOrCancelJsonPacket node = (StopOrCancelJsonPacket) json;
				R66Result resulttest;
				if (node.getRequested() == null || node.getRequester() == null || node.getSpecialid() == DbConstant.ILLEGALVALUE) {
					ErrorCode code = ErrorCode.CommandNotFound;
					resulttest = new R66Result(session, true,
							code, session.getRunner());
					result.addItem(JSON_DETAIL, "Not enough argument passed to identify a transfer");
					setError(handler, result, node, resulttest.code);
				} else {
					String reqd = node.getRequested();
					String reqr = node.getRequester();
					long id = node.getSpecialid();
					resulttest = serverHandler.stopOrCancel(node.getRequestUserPacket(), reqd, reqr, id);
					setOk(handler, result, node, resulttest.code);
				}
			} else if (json instanceof TransferRequestJsonPacket && method == METHOD.POST) {
				TransferRequestJsonPacket node = (TransferRequestJsonPacket) json;
				R66Result r66result = serverHandler.transferRequest(node);
				if (serverHandler.isCodeValid(r66result.code)) {
					setOk(handler, result, node, r66result.code);
				} else {
					result.addItem(JSON_DETAIL, "New Transfer cannot be registered");
					setError(handler, result, r66result.code);
				}
			} else {
				logger.info("Validation is ignored: " + json);
				result.addItem(JSON_DETAIL, "Unknown command");
				setError(handler, result, json, ErrorCode.Unknown);
			}
		} catch (OpenR66ProtocolNotAuthenticatedException e) {
			throw new HttpInvalidAuthenticationException(e);
		} catch (OpenR66ProtocolPacketException e) {
			throw new HttpIncorrectRequestException(e);
		} catch (OpenR66ProtocolNoDataException e) {
			throw new HttpIncorrectRequestException(e);
		}
	}
	
	protected ArrayNode getDetailedAllow() {
		ArrayNode node = JsonHandler.createArrayNode();
		
		ObjectNode node2 = node.addObject().putObject(METHOD.GET.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "GetTransferInformation");
		InformationJsonPacket node3 = new InformationJsonPacket();
		node3.setComment("Information on Transfer request (GET)");
		node2 = node2.putObject(RestArgument.JSON_JSON);
		try {
			node2.putAll(node3.createObjectNode());
		} catch (OpenR66ProtocolPacketException e) {
		}

		node2 = node.addObject().putObject(METHOD.PUT.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "RestartTransfer");
		RestartTransferJsonPacket node4 = new RestartTransferJsonPacket();
		node4.setComment("Restart Transfer request (PUT)");
		node4.setRequested("Requested host");
		node4.setRequester("Requester host");
		node4.setRestarttime(new Date());
		node2 = node2.putObject(RestArgument.JSON_JSON);
		try {
			node2.putAll(node4.createObjectNode());
		} catch (OpenR66ProtocolPacketException e) {
		}

		node2 = node.addObject().putObject(METHOD.PUT.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "StopOrCancelTransfer");
		StopOrCancelJsonPacket node5 = new StopOrCancelJsonPacket();
		node5.setComment("Stop Or Cancel request (PUT)");
		node5.setRequested("Requested host");
		node5.setRequester("Requester host");
		node2 = node2.putObject(RestArgument.JSON_JSON);
		try {
			node2.putAll(node5.createObjectNode());
		} catch (OpenR66ProtocolPacketException e) {
		}
		
		node2 = node.addObject().putObject(METHOD.POST.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "CreateTransfer");
		TransferRequestJsonPacket node6 = new TransferRequestJsonPacket();
		node6.setComment("Transfer Request (POST)");
		node6.setFilename("Filename");
		node6.setFileInformation("File information");
		node6.setRequested("Requested host");
		node6.setRulename("Rulename");
		node6.setStart(new Date());
		node2 = node2.putObject(RestArgument.JSON_JSON);
		try {
			node2.putAll(node6.createObjectNode());
		} catch (OpenR66ProtocolPacketException e) {
		}
		
		node2 = node.addObject().putObject(METHOD.OPTIONS.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.OPTIONS.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);

		return node;
	}
}
