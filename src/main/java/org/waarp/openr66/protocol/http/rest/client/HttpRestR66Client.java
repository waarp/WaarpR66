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
package org.waarp.openr66.protocol.http.rest.client;


import org.jboss.netty.channel.ChannelPipelineFactory;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.json.JsonHandler;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.gateway.kernel.rest.client.HttpRestClientHelper;
import org.waarp.gateway.kernel.rest.client.RestFuture;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author "Frederic Bregier"
 *
 */
public class HttpRestR66Client extends HttpRestClientHelper {

	/**
	 * @param baseUri
	 * @param nbclient
	 * @param timeout
	 * @param pipelineFactory
	 */
	public HttpRestR66Client(String baseUri, int nbclient, long timeout,
			ChannelPipelineFactory pipelineFactory) {
		super(baseUri, nbclient, timeout, pipelineFactory);
	}

	/**
	 * 
	 * @param arg
	 * @return the associated RESTHANDLERS if any, else null
	 */
	public RESTHANDLERS getRestHandler(RestArgument arg) {
		ObjectNode node = arg.getAnswer();
		String model = node.path(AbstractDbData.JSON_MODEL).asText();
		if (model != null && ! model.isEmpty()) {
			return RESTHANDLERS.valueOf(model);
		}
		return null;
	}
	
	/**
	 * 
	 * @param arg
	 * @return the primary property value associated with the Model, else null
	 */
	public String getPrimaryProperty(RestArgument arg) {
		ObjectNode answer = arg.getAnswer();
		String model = answer.path(AbstractDbData.JSON_MODEL).asText();
		if (model != null && ! model.isEmpty()) {
			RESTHANDLERS dbdata = RESTHANDLERS.valueOf(model);
			DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) dbdata.getRestMethodHandler();
			return answer.path(handler.getPrimaryPropertyName()).asText();
		}
		return null;
	}

	/**
	 * 
	 * @param dbSession
	 * @param future
	 * @return the DbData allocated from result if any, else null
	 * @throws HttpIncorrectRequestException
	 */
	@SuppressWarnings("unchecked")
	public AbstractDbData getDbDataFromFuture(DbSession dbSession, RestFuture future) throws HttpIncorrectRequestException {
		if (future.getRestArgument() != null) {
			RestArgument arg = future.getRestArgument();
			ObjectNode node = arg.getAnswer();
			String model = node.path(AbstractDbData.JSON_MODEL).asText();
			if (model != null && ! model.isEmpty()) {
				RESTHANDLERS rmodel = RESTHANDLERS.valueOf(model);
				try {
					return (AbstractDbData) rmodel.clasz.getConstructor(DbSession.class, ObjectNode.class).newInstance(dbSession, node);
				} catch (Exception e) {
					throw new HttpIncorrectRequestException(e);
				}
			}
		}
		return null;
	}

	/**
	 * 
	 * @param future
	 * @return the JsonPacket from result if any, else null
	 * @throws HttpIncorrectRequestException
	 */
	public JsonPacket getJsonPacketFromFuture(RestFuture future) throws HttpIncorrectRequestException {
		if (future.getRestArgument() != null) {
			RestArgument arg = future.getRestArgument();
			ObjectNode node = arg.getAnswer();
			try {
				return JsonPacket.createFromBuffer(JsonHandler.writeAsString(node));
			} catch (Exception e) {
				throw new HttpIncorrectRequestException(e);
			}
		}
		return null;
	}

}
