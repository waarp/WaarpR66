/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.database.data;

import java.sql.Types;
import java.util.concurrent.ConcurrentHashMap;

import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.openr66.commander.CommanderNoDb;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Configuration Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbHostConfiguration extends AbstractDbData {
	public static enum Columns {
		BUSINESS,
		ROLES,
		ALIASES,
		OTHERS,
		UPDATEDINFO,
		HOSTID
	}

	public static final int[] dbTypes = {
			Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR,
			Types.INTEGER, Types.VARCHAR };

	public static final String table = " HOSTCONFIG ";

	/**
	 * HashTable in case of lack of database
	 */
	private static final ConcurrentHashMap<String, DbHostConfiguration> dbR66HostConfigurationHashMap =
			new ConcurrentHashMap<String, DbHostConfiguration>();

	private String hostid;

	private String business;

	private String roles;

	private String aliases;
	
	private String others;

	private int updatedInfo = UpdatedInfo.UNKNOWN
			.ordinal();

	// ALL TABLE SHOULD IMPLEMENT THIS
	public static final int NBPRKEY = 1;

	protected static final String selectAllFields = Columns.BUSINESS
			.name()
			+
			","
			+
			Columns.ROLES
					.name()
			+
			","
			+
			Columns.ALIASES
					.name()
			+
			","
			+
			Columns.OTHERS
					.name()
			+
			","
			+ Columns.UPDATEDINFO
					.name()
			+ ","
			+ Columns.HOSTID
					.name();

	protected static final String updateAllFields = Columns.BUSINESS
			.name()
			+
			"=?,"
			+
			Columns.ROLES
					.name()
			+
			"=?,"
			+
			Columns.ALIASES
					.name()
			+
			"=?,"
			+
			Columns.OTHERS
					.name()
			+
			"=?,"
			+
			Columns.UPDATEDINFO
					.name()
			+
			"=?";

	protected static final String insertAllValues = " (?,?,?,?,?,?) ";

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#initObject()
	 */
	@Override
	protected void initObject() {
		primaryKey = new DbValue[] { new DbValue(hostid, Columns.HOSTID
				.name()) };
		otherFields = new DbValue[] {
				new DbValue(business, Columns.BUSINESS.name(), true),
				new DbValue(roles, Columns.ROLES.name(), true),
				new DbValue(aliases, Columns.ALIASES.name(), true),
				new DbValue(others, Columns.OTHERS.name(), true),
				new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };
		allFields = new DbValue[] {
				otherFields[0], otherFields[1], otherFields[2], otherFields[3],
				otherFields[4], primaryKey[0] };
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#getSelectAllFields()
	 */
	@Override
	protected String getSelectAllFields() {
		return selectAllFields;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#getTable()
	 */
	@Override
	protected String getTable() {
		return table;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#getInsertAllValues()
	 */
	@Override
	protected String getInsertAllValues() {
		return insertAllValues;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#getUpdateAllFields()
	 */
	@Override
	protected String getUpdateAllFields() {
		return updateAllFields;
	}

	@Override
	protected void setToArray() {
		allFields[Columns.HOSTID.ordinal()].setValue(hostid);
		if (business == null) {
			business = "";
		}
		allFields[Columns.BUSINESS.ordinal()].setValue(business);
		if (roles == null) {
			roles = "";
		}
		allFields[Columns.ROLES.ordinal()]
				.setValue(roles);
		if (aliases == null) {
			aliases = "";
		}
		allFields[Columns.ALIASES.ordinal()]
				.setValue(aliases);
		if (others == null) {
			others = "";
		}
		allFields[Columns.OTHERS.ordinal()]
				.setValue(others);
		allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
	}

	@Override
	protected void setFromArray() throws WaarpDatabaseSqlException {
		hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
		business = (String) allFields[Columns.BUSINESS.ordinal()].getValue();
		roles = (String) allFields[Columns.ROLES.ordinal()].getValue();
		aliases = (String) allFields[Columns.ALIASES.ordinal()].getValue();
		others = (String) allFields[Columns.OTHERS.ordinal()].getValue();
		updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#getWherePrimaryKey()
	 */
	@Override
	protected String getWherePrimaryKey() {
		return primaryKey[0].column + " = ? ";
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#setPrimaryKey()
	 */
	@Override
	protected void setPrimaryKey() {
		primaryKey[0].setValue(hostid);
	}

	/**
	 * @param dbSession
	 * @param hostid
	 * @param business
	 *            Business configuration
	 * @param roles
	 *            Roles configuration
	 * @param aliases
	 *            Aliases configuration
	 * @param others
	 *            Other configuration
	 */
	public DbHostConfiguration(DbSession dbSession, String hostid, String business, String roles, String aliases, String others) {
		super(dbSession);
		this.hostid = hostid;
		this.business = business;
		this.roles = roles;
		this.aliases = aliases;
		this.others = others;
		setToArray();
		isSaved = false;
	}

	/**
	 * @param dbSession
	 * @param hostid
	 * @throws WaarpDatabaseException
	 */
	public DbHostConfiguration(DbSession dbSession, String hostid) throws WaarpDatabaseException {
		super(dbSession);
		this.hostid = hostid;
		// load from DB
		select();
	}

	/**
	 * @return the hostid
	 */
	public String getHostid() {
		return hostid;
	}

	/**
	 * @return the business
	 */
	public String getBusiness() {
		return business;
	}

	/**
	 * @param business the business to set
	 */
	public void setBusiness(String business) {
		this.business = business == null ? "" : business;
		allFields[Columns.BUSINESS.ordinal()].setValue(business);
		isSaved = false;
	}

	/**
	 * @return the roles
	 */
	public String getRoles() {
		return roles;
	}

	/**
	 * @param roles the roles to set
	 */
	public void setRoles(String roles) {
		this.roles = roles == null ? "" : roles;
		allFields[Columns.ROLES.ordinal()].setValue(roles);
		isSaved = false;
	}

	/**
	 * @return the aliases
	 */
	public String getAliases() {
		return aliases;
	}

	/**
	 * @param aliases the aliases to set
	 */
	public void setAliases(String aliases) {
		this.aliases = aliases == null ? "" : aliases;
		allFields[Columns.ALIASES.ordinal()].setValue(aliases);
		isSaved = false;
	}

	/**
	 * @return the others
	 */
	public String getOthers() {
		return others;
	}

	/**
	 * @param others the others to set
	 */
	public void setOthers(String others) {
		this.others = others == null ? "" : others;
		allFields[Columns.OTHERS.ordinal()].setValue(others);
		isSaved = false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#delete()
	 */
	@Override
	public void delete() throws WaarpDatabaseException {
		if (dbSession == null) {
			dbR66HostConfigurationHashMap.remove(this.hostid);
			isSaved = false;
			return;
		}
		super.delete();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#insert()
	 */
	@Override
	public void insert() throws WaarpDatabaseException {
		if (isSaved) {
			return;
		}
		if (dbSession == null) {
			dbR66HostConfigurationHashMap.put(this.hostid, this);
			isSaved = true;
			if (this.updatedInfo == UpdatedInfo.TOSUBMIT.ordinal()) {
				CommanderNoDb.todoList.add(this);
			}
			return;
		}
		super.insert();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#exist()
	 */
	@Override
	public boolean exist() throws WaarpDatabaseException {
		if (dbSession == null) {
			return dbR66HostConfigurationHashMap.containsKey(hostid);
		}
		return super.exist();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#select()
	 */
	@Override
	public void select() throws WaarpDatabaseException {
		if (dbSession == null) {
			DbHostConfiguration conf = dbR66HostConfigurationHashMap.get(this.hostid);
			if (conf == null) {
				throw new WaarpDatabaseNoDataException("No row found");
			} else {
				// copy info
				for (int i = 0; i < allFields.length; i++) {
					allFields[i].value = conf.allFields[i].value;
				}
				setFromArray();
				isSaved = true;
				return;
			}
		}
		super.select();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#update()
	 */
	@Override
	public void update() throws WaarpDatabaseException {
		if (isSaved) {
			return;
		}
		if (dbSession == null) {
			dbR66HostConfigurationHashMap.put(this.hostid, this);
			isSaved = true;
			if (this.updatedInfo == UpdatedInfo.TOSUBMIT.ordinal()) {
				CommanderNoDb.todoList.add(this);
			}
			return;
		}
		super.update();
	}

	/**
	 * Private constructor for Commander only
	 */
	private DbHostConfiguration(DbSession session) {
		super(session);
	}

	/**
	 * For instance from Commander when getting updated information
	 * 
	 * @param preparedStatement
	 * @return the next updated Configuration
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws WaarpDatabaseSqlException
	 */
	public static DbHostConfiguration getFromStatement(DbPreparedStatement preparedStatement)
			throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
		DbHostConfiguration dbConfiguration = new DbHostConfiguration(preparedStatement.getDbSession());
		dbConfiguration.getValues(preparedStatement, dbConfiguration.allFields);
		dbConfiguration.setFromArray();
		dbConfiguration.isSaved = true;
		return dbConfiguration;
	}

	/**
	 * 
	 * @return the DbPreparedStatement for getting Updated Object
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws WaarpDatabaseSqlException
	 */
	public static DbPreparedStatement getUpdatedPrepareStament(DbSession session)
			throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
		String request = "SELECT " + selectAllFields;
		request += " FROM " + table +
				" WHERE " + Columns.UPDATEDINFO.name() + " = " +
				AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal() +
				" AND " + Columns.HOSTID.name() + " = '" + Configuration.configuration.HOST_ID
				+ "'";
		DbPreparedStatement prep = new DbPreparedStatement(session, request);
		return prep;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#changeUpdatedInfo(UpdatedInfo)
	 */
	@Override
	public void changeUpdatedInfo(UpdatedInfo info) {
		if (updatedInfo != info.ordinal()) {
			updatedInfo = info.ordinal();
			allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
			isSaved = false;
		}
	}

	/**
	 * Update configuration according to new values
	 */
	public void updateConfiguration() {
		FileBasedConfiguration.updateHostConfiguration(Configuration.configuration, this);
	}

	/**
	 * 
	 * @return True if this Configuration refers to the current host
	 */
	public boolean isOwnConfiguration() {
		return this.hostid.equals(Configuration.configuration.HOST_ID);
	}
}
