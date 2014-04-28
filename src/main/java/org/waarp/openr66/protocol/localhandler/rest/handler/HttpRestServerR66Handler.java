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


import static org.waarp.openr66.context.R66FiniteDualStates.SHUTDOWN;

import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler.COMMAND_TYPE;
import org.waarp.gateway.kernel.rest.HttpRestHandler.METHOD;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolShutdownException;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.ShutdownOrBlockJsonPacket;
import org.waarp.openr66.protocol.localhandler.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Server Http REST interface: http://host/server?... + ShutdownOrBlockJsonPacket as PUT
 * @author "Frederic Bregier"
 *
 */
public class HttpRestServerR66Handler extends HttpRestAbstractR66Handler {
	
	public static final String BASEURI = "server";
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestServerR66Handler.class);
   
	/**
	 * @param path
	 * @param method
	 */
	public HttpRestServerR66Handler() {
		super(BASEURI, METHOD.PUT);
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
			if (json instanceof ShutdownOrBlockJsonPacket) {//
				ShutdownOrBlockJsonPacket node = (ShutdownOrBlockJsonPacket) json;
				if (node.isShutdownOrBlock()) {
					// Shutdown
					session.newState(SHUTDOWN);
					serverHandler.shutdown(node.getKey(), node.isRestartOrBlock());
					setOk(handler, result, json, ErrorCode.Shutdown);
				} else {
					// Block
					R66Result r66result = serverHandler.blockRequest(node.getKey(), node.isRestartOrBlock());
					node.setComment((node.isRestartOrBlock() ? "Block" : "Unblock")+" new request");
					result.addItem(JSON_DETAIL, (node.isRestartOrBlock() ? "Block" : "Unblock")+" new request");
					setOk(handler, result, json, r66result.code);
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
		} catch (OpenR66ProtocolBusinessException e) {
			throw new HttpIncorrectRequestException(e);
		} catch (OpenR66ProtocolShutdownException e) {
			R66ShutdownHook.shutdownWillStart();
			logger.warn(Messages.getString("LocalServerHandler.0") + //$NON-NLS-1$
					serverHandler.getSession().getAuth().getUser());
			// dont'close, thread will do
			ChannelUtils.startShutdown();
		}
	}
	
	protected ArrayNode getDetailedAllow() {
		ArrayNode node = JsonHandler.createArrayNode();
		
		ObjectNode node2 = node.addObject().putObject(METHOD.PUT.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2.put(RestArgument.JSON_COMMAND, "ShutdownOrBlock");
		ShutdownOrBlockJsonPacket node3 = new ShutdownOrBlockJsonPacket();
		node3.setComment("Shutdown Or Block request (PUT)");
		node3.setKey("Key".getBytes(WaarpStringUtils.UTF8));
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
