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

import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.multipart.FileUpload;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.HttpRestHandler.METHOD;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.gateway.kernel.rest.RestMethodHandler;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.rest.HttpRestR66Handler;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * @author "Frederic Bregier"
 *
 */
public abstract class HttpRestAbstractR66Handler extends RestMethodHandler {
	
	public static final String JSON_RESULT = "result";
	public static final String JSON_MESSAGE = "message";
	public static final String JSON_MESSAGECODE = "messagecode";
	public static final String JSON_DETAIL = "detail";
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestAbstractR66Handler.class);
   
	/**
	 * @param path
	 * @param method
	 */
	public HttpRestAbstractR66Handler(String path, METHOD... method) {
		super(path, true, method);
	}
	
	@Override
	public void checkHandlerSessionCorrectness(HttpRestHandler handler, 
			RestArgument arguments, RestArgument result) {
		// no check to do here ?
		logger.debug("debug");
	}

	@Override
	public void getFileUpload(HttpRestHandler handler, FileUpload data, RestArgument arguments,
			RestArgument result) throws HttpIncorrectRequestException {
		// should not be
		logger.debug("debug: "+data.getName()+":"+data.getHttpDataType().name());
	}

	protected void setError(HttpRestHandler handler, RestArgument result, ErrorCode code) {
		handler.setStatus(HttpResponseStatus.BAD_REQUEST);
		handler.setWillClose(true);
		result.addItem(JSON_MESSAGE, code.mesg);
		result.addItem(JSON_MESSAGECODE, ""+code.code);
	}

	protected void setError(HttpRestHandler handler, RestArgument result, JsonPacket packet, ErrorCode code) {
		handler.setStatus(HttpResponseStatus.BAD_REQUEST);
		result.addItem(JSON_MESSAGE, code.mesg);
		result.addItem(JSON_MESSAGECODE, ""+code.code);
		if (packet != null) {
			try {
				result.getAnswer().put(JSON_RESULT, packet.createObjectNode());
			} catch (OpenR66ProtocolPacketException e) {
			}
		}
	}

	protected void setOk(HttpRestHandler handler, RestArgument result, JsonPacket packet, ErrorCode code) {
		handler.setStatus(HttpResponseStatus.OK);
		result.addItem(JSON_MESSAGE, code.mesg);
		result.addItem(JSON_MESSAGECODE, ""+code.code);
		if (packet != null) {
			try {
				result.getAnswer().put(JSON_RESULT, packet.createObjectNode());
			} catch (OpenR66ProtocolPacketException e) {
				result.addItem(JSON_DETAIL, "serialization impossible");
			}
		}
	}

	@Override
	public HttpResponseStatus handleException(HttpRestHandler handler, RestArgument arguments,
			RestArgument result, Object body, Exception exception) {
		((HttpRestR66Handler) handler).serverHandler.getSession().newState(ERROR);
		return super.handleException(handler, arguments, result, body, exception);
	}

	@Override
	public ChannelFuture sendResponse(HttpRestHandler handler, Channel channel, RestArgument arguments,
			RestArgument result, Object body, HttpResponseStatus status) {
		HttpResponse response = handler.getResponse();
		if (status == HttpResponseStatus.UNAUTHORIZED) {
			ChannelFuture future = channel.write(response);
			return future;
		}
		response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "application/json");
		response.headers().add(HttpHeaders.Names.REFERER, handler.getRequest().getUri());
		String answer = result.toString();
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(answer.getBytes(WaarpStringUtils.UTF8));
		response.headers().add(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
		response.setContent(buffer);
		logger.debug("Will write: {}", body);
		ChannelFuture future = channel.write(response);
		if (handler.isWillClose()) {
			System.err.println("Will close session in HttpRestAbstractR66Handler");
			return future;
		}
		return null;
	}

	@Override
	public Object getBody(HttpRestHandler handler, ChannelBuffer body, RestArgument arguments,
			RestArgument result) throws HttpIncorrectRequestException {
		JsonPacket packet = null;
		try {
			String json = body.toString(WaarpStringUtils.UTF8);
			packet = JsonPacket.createFromBuffer(json);
		} catch (JsonParseException e) {
			logger.warn("Error: "+body.toString(WaarpStringUtils.UTF8), e);
			throw new HttpIncorrectRequestException(e);
		} catch (JsonMappingException e) {
			logger.warn("Error", e);
			throw new HttpIncorrectRequestException(e);
		} catch (IOException e) {
			logger.warn("Error", e);
			throw new HttpIncorrectRequestException(e);
		} catch (UnsupportedCharsetException e) {
			logger.warn("Error", e);
			throw new HttpIncorrectRequestException(e);
		}
		return packet;
	}
}
