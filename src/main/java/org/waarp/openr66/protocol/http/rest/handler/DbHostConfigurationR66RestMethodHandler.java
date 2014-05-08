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
package org.waarp.openr66.protocol.http.rest.handler;

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
import org.waarp.openr66.database.data.DbHostConfiguration;
import org.waarp.openr66.database.data.DbHostConfiguration.Columns;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * DbHostConfiguration Rest handler
 * @author "Frederic Bregier"
 *
 */
public class DbHostConfigurationR66RestMethodHandler extends DataModelRestMethodHandler<DbHostConfiguration> {
	public static enum FILTER_ARGS {
		HOSTID("host name subtext"),
		BUSINESS("BUSINESS information subtext"),
		ROLES("ROLES information subtext"),
		ALIASES("ALIASES information subtext"),
		OTHERS("OTHERS information subtext");
		
		public String type;
		FILTER_ARGS(String type) {
			this.type = type;
		}
	}
	/**
	 * @param name
	 * @param allowDelete
	 */
	public DbHostConfigurationR66RestMethodHandler(String name, boolean allowDelete) {
		super(name, allowDelete);
	}

	protected DbHostConfiguration getItem(HttpRestHandler handler, RestArgument arguments,
			RestArgument result, Object body) throws HttpIncorrectRequestException,
			HttpInvalidAuthenticationException, HttpNotFoundRequestException {
		ObjectNode arg = arguments.getUriArgs().deepCopy();
		arg.putAll(arguments.getBody());
		try {
			JsonNode node = RestArgument.getId(arg);
			String id;
			if (node.isMissingNode()) {
				// shall not be but continue however
				id = arg.path(DbHostConfiguration.Columns.HOSTID.name()).asText();
			} else {
				id = node.asText();
			}
			return new DbHostConfiguration(handler.getDbSession(), id);
		} catch (WaarpDatabaseException e) {
			throw new HttpNotFoundRequestException("Issue while reading from database "+arg, e);
		}
	}

	@Override
	protected DbHostConfiguration createItem(HttpRestHandler handler, RestArgument arguments,
			RestArgument result, Object body) throws HttpIncorrectRequestException,
			HttpInvalidAuthenticationException {
		ObjectNode arg = arguments.getUriArgs().deepCopy();
		arg.putAll(arguments.getBody());
		try {
			return new DbHostConfiguration(handler.getDbSession(), arg);
		} catch (WaarpDatabaseException e) {
			throw new HttpIncorrectRequestException("Issue while inserting into database", e);
		}
	}

	@Override
	protected DbPreparedStatement getPreparedStatement(HttpRestHandler handler,
			RestArgument arguments, RestArgument result, Object body)
			throws HttpIncorrectRequestException, HttpInvalidAuthenticationException {
		ObjectNode arg = arguments.getUriArgs().deepCopy();
		arg.putAll(arguments.getBody());
		String hostid = arg.path(FILTER_ARGS.HOSTID.name()).asText();
		if (hostid == null || hostid.isEmpty()) {
			hostid = null;
		}
		String business = arg.path(FILTER_ARGS.BUSINESS.name()).asText();
		if (business == null || business.isEmpty()) {
			business = null;
		}
		String role = arg.path(FILTER_ARGS.ROLES.name()).asText();
		if (role == null || role.isEmpty()) {
			role = null;
		}
		String alias = arg.path(FILTER_ARGS.ALIASES.name()).asText();
		if (alias == null || alias.isEmpty()) {
			alias = null;
		}
		String other = arg.path(FILTER_ARGS.OTHERS.name()).asText();
		if (other == null || other.isEmpty()) {
			other = null;
		}
		try {
			return DbHostConfiguration.getFilterPrepareStament(handler.getDbSession(),
					hostid, business, role, alias, other);
		} catch (WaarpDatabaseNoConnectionException e) {
			throw new HttpIncorrectRequestException("Issue while reading from database", e);
		} catch (WaarpDatabaseSqlException e) {
			throw new HttpIncorrectRequestException("Issue while reading from database", e);
		}
	}

	@Override
	protected DbHostConfiguration getItemPreparedStatement(DbPreparedStatement statement)
			throws HttpIncorrectRequestException, HttpNotFoundRequestException {
		try {
			return DbHostConfiguration.getFromStatement(statement);
		} catch (WaarpDatabaseNoConnectionException e) {
			throw new HttpIncorrectRequestException("Issue while selecting from database", e);
		} catch (WaarpDatabaseSqlException e) {
			throw new HttpNotFoundRequestException("Issue while selecting from database", e);
		}
	}

	@Override
	protected ArrayNode getDetailedAllow() {
		ArrayNode node = JsonHandler.createArrayNode();
		
		ObjectNode node2;
		node2 = RestArgument.fillDetailedAllow(METHOD.GET, this.path+"/id", COMMAND_TYPE.GET.name(), 
				JsonHandler.createObjectNode().put(DbHostConfiguration.Columns.HOSTID.name(), "HostId as VARCHAR in URI as "+this.path+"/id"));
		node.add(node2);

		ObjectNode node3 = JsonHandler.createObjectNode();
		for (FILTER_ARGS arg : FILTER_ARGS.values()) {
			node3.put(arg.name(), arg.type);
		}
		node2 = RestArgument.fillDetailedAllow(METHOD.GET, this.path, COMMAND_TYPE.MULTIGET.name(), 
				node3);
		node.add(node2);

		node3 = JsonHandler.createObjectNode();
		node3.put(DbHostConfiguration.Columns.HOSTID.name(), "HostId as VARCHAR in URI as "+this.path+"/id"); 
		DbValue []values = DbHostConfiguration.getAllType();
		for (DbValue dbValue : values) {
			if (dbValue.column.equalsIgnoreCase(DbHostConfiguration.Columns.HOSTID.name())) {
				continue;
			}
			node3.put(dbValue.column, dbValue.getType());
		}
		node2 = RestArgument.fillDetailedAllow(METHOD.PUT, this.path+"/id", COMMAND_TYPE.UPDATE.name(), 
				node3);
		node.add(node2);
		
		node3 = JsonHandler.createObjectNode();
		node3.put(DbHostConfiguration.Columns.HOSTID.name(), "HostId as VARCHAR in URI as "+this.path+"/id"); 
		node2 = RestArgument.fillDetailedAllow(METHOD.DELETE, this.path+"/id", COMMAND_TYPE.DELETE.name(), 
				node3);
		node.add(node2);

		node3 = JsonHandler.createObjectNode();
		for (DbValue dbValue : values) {
			node3.put(dbValue.column, dbValue.getType());
		}
		node2 = RestArgument.fillDetailedAllow(METHOD.POST, this.path, COMMAND_TYPE.CREATE.name(), 
				node3);
		node.add(node2);
				
		node2 = RestArgument.fillDetailedAllow(METHOD.OPTIONS, this.path, COMMAND_TYPE.OPTIONS.name(), null);
		node.add(node2);

		return node;
	}
	@Override
	public String getPrimaryPropertyName() {
		return Columns.HOSTID.name();
	}

}
