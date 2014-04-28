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

import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.json.JsonHandler;
import org.waarp.gateway.kernel.exception.HttpIncorrectRequestException;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.exception.HttpNotFoundRequestException;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.gateway.kernel.rest.HttpRestHandler.METHOD;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author "Frederic Bregier"
 *
 */
public class DbTaskRunnerR66RestMethodHandler extends DataModelRestMethodHandler<DbTaskRunner> {

	public static enum FILTER_ARGS {
		LIMIT("number"),
		ORDERBYID("boolean"),
		STARTID("transfer id"),
		STOPID("transfer id"),
		IDRULE("rule name"),
		PARTNER("PARTNER name"),
		PENDING("boolean"),
		INTRANSFER("boolean"),
		ERROR("boolean"),
		DONE("boolean"),
		ALL("boolean"),
		STARTTRANS("Date in ISO 8601 format or ms"),
		STOPTRANS("Date in ISO 8601 format or ms");
		
		public String type;
		FILTER_ARGS(String type) {
			this.type = type;
		}
	}
	/**
	 * @param name
	 * @param allowDelete
	 */
	public DbTaskRunnerR66RestMethodHandler(String name, boolean allowDelete) {
		super(name, allowDelete);
	}

	protected DbTaskRunner getItem(HttpRestHandler handler, RestArgument arguments,
			RestArgument result, Object body) throws HttpIncorrectRequestException,
			HttpInvalidAuthenticationException, HttpNotFoundRequestException {
		ObjectNode arg = arguments.getBody();
		try {
			JsonNode node = arg.path(JSON_ID);
			long id;
			if (node.isMissingNode()) {
				// shall not be but continue however
				id = arg.path(DbTaskRunner.Columns.SPECIALID.name()).asLong();
			} else {
				id = node.asLong();
			}
			return new DbTaskRunner(DbConstant.admin.session, id, 
					arg.path(DbTaskRunner.Columns.REQUESTER.name()).asText(), 
					arg.path(DbTaskRunner.Columns.REQUESTED.name()).asText());
		} catch (WaarpDatabaseException e) {
			throw new HttpNotFoundRequestException("Issue while reading from database", e);
		}
	}

	@Override
	protected DbTaskRunner createItem(HttpRestHandler handler, RestArgument arguments,
			RestArgument result, Object body) throws HttpIncorrectRequestException,
			HttpInvalidAuthenticationException {
		ObjectNode arg = arguments.getBody();
		try {
			return new DbTaskRunner(DbConstant.admin.session, arg);
		} catch (WaarpDatabaseException e) {
			throw new HttpIncorrectRequestException("Issue while inserting into database", e);
		}
	}

	@Override
	protected DbPreparedStatement getPreparedStatement(HttpRestHandler handler,
			RestArgument arguments, RestArgument result, Object body)
			throws HttpIncorrectRequestException, HttpInvalidAuthenticationException {
		ObjectNode arg = arguments.getUriArgs();
		int limit = arg.path(FILTER_ARGS.LIMIT.name()).asInt(0);
		boolean orderBySpecialId = arg.path(FILTER_ARGS.ORDERBYID.name()).asBoolean(false);
		JsonNode node = arg.path(FILTER_ARGS.STARTID.name());
		String startid = null;
		if (! node.isMissingNode()) {
			startid = node.asText();
		}
		if (startid == null || startid.isEmpty()) {
			startid = null;
		}
		node = arg.path(FILTER_ARGS.STOPID.name());
		String stopid = null;
		if (! node.isMissingNode()) {
			stopid = node.asText();
		}
		if (stopid == null || stopid.isEmpty()) {
			stopid = null;
		}
		String rule = arg.path(FILTER_ARGS.IDRULE.name()).asText();
		if (rule == null || rule.isEmpty()) {
			rule = null;
		}
		String req = arg.path(FILTER_ARGS.PARTNER.name()).asText();
		if (req == null || req.isEmpty()) {
			req = null;
		}
		boolean pending = arg.path(FILTER_ARGS.PENDING.name()).asBoolean(false);
		boolean transfer = arg.path(FILTER_ARGS.INTRANSFER.name()).asBoolean(false);
		boolean error = arg.path(FILTER_ARGS.ERROR.name()).asBoolean(false);
		boolean done = arg.path(FILTER_ARGS.DONE.name()).asBoolean(false);
		boolean all = arg.path(FILTER_ARGS.ALL.name()).asBoolean(false);
		Timestamp start = null;
		node = arg.path(FILTER_ARGS.STARTTRANS.name());
		if (! node.isMissingNode()) {
			long val = node.asLong();
			if (val == 0) {
				DateTime received = DateTime.parse(node.asText());
				val = received.getMillis();
			}
			start = new Timestamp(val);
		}
		Timestamp stop = null;
		node = arg.path(FILTER_ARGS.STOPTRANS.name());
		if (! node.isMissingNode()) {
			long val = node.asLong();
			if (val == 0) {
				DateTime received = DateTime.parse(node.asText());
				val = received.getMillis();
			}
			stop = new Timestamp(val);
		}
		try {
			return DbTaskRunner.getFilterPrepareStatement(DbConstant.admin.session, 
					limit, orderBySpecialId, startid, stopid, start, stop, rule, req, pending, transfer, error, done, all);
		} catch (WaarpDatabaseNoConnectionException e) {
			throw new HttpIncorrectRequestException("Issue while reading from database", e);
		} catch (WaarpDatabaseSqlException e) {
			throw new HttpIncorrectRequestException("Issue while reading from database", e);
		}
	}

	@Override
	protected DbTaskRunner getItemPreparedStatement(DbPreparedStatement statement)
			throws HttpIncorrectRequestException, HttpNotFoundRequestException {
		try {
			return DbTaskRunner.getFromStatement(statement);
		} catch (WaarpDatabaseNoConnectionException e) {
			throw new HttpIncorrectRequestException("Issue while selecting from database", e);
		} catch (WaarpDatabaseSqlException e) {
			throw new HttpNotFoundRequestException("Issue while selecting from database", e);
		}
	}

	@Override
	protected ArrayNode getDetailedAllow() {
		ArrayNode node = JsonHandler.createArrayNode();
		
		ObjectNode node2 = node.addObject().putObject(METHOD.GET.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.GET.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path+"/id");
		node2.put(DbTaskRunner.Columns.SPECIALID.name(), "Special Id in URI as "+this.path+"/id"); 
		node2.put(DbTaskRunner.Columns.REQUESTER.name(), "Partner as requester"); 
		node2.put(DbTaskRunner.Columns.REQUESTED.name(), "Partner as requested");

		node2 = node.addObject().putObject(METHOD.GET.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.MULTIGET.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2 = node2.putObject(RestArgument.JSON_JSON);
		for (FILTER_ARGS arg : FILTER_ARGS.values()) {
			node2.put(arg.name(), arg.type);
		}
		
		node2 = node.addObject().putObject(METHOD.PUT.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.UPDATE.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path+"/id");
		node2.put(DbTaskRunner.Columns.SPECIALID.name(), "Special Id in URI as "+this.path+"/id"); 
		node2.put(DbTaskRunner.Columns.REQUESTER.name(), "Partner as requester"); 
		node2.put(DbTaskRunner.Columns.REQUESTED.name(), "Partner as requested");
		node2 = node2.putObject(RestArgument.JSON_JSON);
		DbValue []values = DbTaskRunner.getAllType();
		for (DbValue dbValue : values) {
			node2.put(dbValue.column, dbValue.getType());
		}
		
		node2 = node.addObject().putObject(METHOD.DELETE.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.DELETE.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path+"/id");
		node2.put(DbTaskRunner.Columns.SPECIALID.name(), "Special Id in URI as "+this.path+"/id"); 
		node2.put(DbTaskRunner.Columns.REQUESTER.name(), "Partner as requester"); 
		node2.put(DbTaskRunner.Columns.REQUESTED.name(), "Partner as requested");
		
		node2 = node.addObject().putObject(METHOD.POST.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.CREATE.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);
		node2 = node2.putObject(RestArgument.JSON_JSON);
		for (DbValue dbValue : values) {
			node2.put(dbValue.column, dbValue.getType());
		}
		
		node2 = node.addObject().putObject(METHOD.OPTIONS.name());
		node2.put(RestArgument.JSON_COMMAND, COMMAND_TYPE.OPTIONS.name());
		node2.put(RestArgument.JSON_PATH, "/"+this.path);

		return node;
	}

}
