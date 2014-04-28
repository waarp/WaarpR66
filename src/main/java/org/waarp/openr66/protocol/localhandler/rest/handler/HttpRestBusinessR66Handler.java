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


import static org.waarp.openr66.context.R66FiniteDualStates.ERROR;

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
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.json.BusinessRequestJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.utils.R66Future;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Business Http REST interface: http://host/business?... + BusinessRequestJsonPacket as GET
 * @author "Frederic Bregier"
 *
 */
public class HttpRestBusinessR66Handler extends HttpRestAbstractR66Handler {
	
	public static final String BASEURI = "business";
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestBusinessR66Handler.class);
   
	/**
	 * @param path
	 * @param method
	 */
	public HttpRestBusinessR66Handler() {
		super(BASEURI, METHOD.GET);
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
		try {
			if (json instanceof BusinessRequestJsonPacket) {//
				BusinessRequestJsonPacket node = (BusinessRequestJsonPacket) json;
				R66Future future = serverHandler.businessRequest(node.isToApplied(), node.getClassName(), node.getArguments(), node.getExtraArguments(), node.getDelay());
				if (future != null && ! future.isSuccess()) {
					R66Result r66result = future.getResult();
					if (r66result == null) {
						r66result = new R66Result(session, false, ErrorCode.ExternalOp, session.getRunner());
					}
					logger.info("Task in Error:" + node.getClassName() + " " + r66result);
					if (!r66result.isAnswered) {
						node.setValidated(false);
						session.newState(ERROR);
					}
					result.addItem(JSON_DETAIL, "Task in Error:" + node.getClassName() + " " + r66result);
					setError(handler, result, r66result.code);
				} else {
					setOk(handler, result, json, ErrorCode.CompleteOk);
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
		}
	}

	protected ArrayNode getDetailedAllow() {
		ArrayNode node = JsonHandler.createArrayNode();
		
		ObjectNode node2 = node.addObject().putObject(METHOD.GET.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "ExecuteBusiness");
		BusinessRequestJsonPacket node3 = new BusinessRequestJsonPacket();
		node3.setComment("Business execution request (GET)");
		node3.setClassName("Class name to execute");
		node3.setArguments("Arguments of the execution");
		node3.setExtraArguments("Extra arguments");
		node2 = node2.putObject(RestArgument.JSON_JSON);
		try {
			node2.putAll(node3.createObjectNode());
		} catch (OpenR66ProtocolPacketException e) {
		}

		node2 = node.addObject().putObject(METHOD.OPTIONS.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.OPTIONS.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		
		return node;
	}
}
