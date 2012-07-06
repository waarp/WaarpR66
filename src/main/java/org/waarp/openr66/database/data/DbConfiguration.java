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
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Configuration Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbConfiguration extends AbstractDbData {
	public static enum Columns {
		READGLOBALLIMIT,
		WRITEGLOBALLIMIT,
		READSESSIONLIMIT,
		WRITESESSIONLIMIT,
		DELAYLIMIT,
		UPDATEDINFO,
		HOSTID
	}

	public static final int[] dbTypes = {
			Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
			Types.BIGINT, Types.INTEGER, Types.VARCHAR };

	public static final String table = " CONFIGURATION ";

	/**
	 * HashTable in case of lack of database
	 */
	private static final ConcurrentHashMap<String, DbConfiguration> dbR66ConfigurationHashMap =
			new ConcurrentHashMap<String, DbConfiguration>();

	private String hostid;

	private long readgloballimit;

	private long writegloballimit;

	private long readsessionlimit;

	private long writesessionlimit;

	private long delayllimit;

	private int updatedInfo = UpdatedInfo.UNKNOWN
			.ordinal();

	// ALL TABLE SHOULD IMPLEMENT THIS
	public static final int NBPRKEY = 1;

	protected static final String selectAllFields = Columns.READGLOBALLIMIT
			.name()
			+
			","
			+
			Columns.WRITEGLOBALLIMIT
					.name()
			+
			","
			+
			Columns.READSESSIONLIMIT
					.name()
			+
			","
			+
			Columns.WRITESESSIONLIMIT
					.name()
			+
			","
			+
			Columns.DELAYLIMIT
					.name()
			+
			","
			+ Columns.UPDATEDINFO
					.name()
			+ ","
			+ Columns.HOSTID
					.name();

	protected static final String updateAllFields = Columns.READGLOBALLIMIT
			.name()
			+
			"=?,"
			+
			Columns.WRITEGLOBALLIMIT
					.name()
			+
			"=?,"
			+
			Columns.READSESSIONLIMIT
					.name()
			+
			"=?,"
			+
			Columns.WRITESESSIONLIMIT
					.name()
			+
			"=?,"
			+
			Columns.DELAYLIMIT
					.name()
			+
			"=?,"
			+
			Columns.UPDATEDINFO
					.name()
			+
			"=?";

	protected static final String insertAllValues = " (?,?,?,?,?,?,?) ";

	/*
	 * (non-Javadoc)
	 * @see org.waarp.common.database.data.AbstractDbData#initObject()
	 */
	@Override
	protected void initObject() {
		primaryKey = new DbValue[] { new DbValue(hostid, Columns.HOSTID
				.name()) };
		otherFields = new DbValue[] {
				new DbValue(readgloballimit, Columns.READGLOBALLIMIT.name()),
				new DbValue(writegloballimit, Columns.WRITEGLOBALLIMIT.name()),
				new DbValue(readsessionlimit, Columns.READSESSIONLIMIT.name()),
				new DbValue(writesessionlimit, Columns.WRITESESSIONLIMIT.name()),
				new DbValue(delayllimit, Columns.DELAYLIMIT.name()),
				new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };
		allFields = new DbValue[] {
				otherFields[0], otherFields[1], otherFields[2], otherFields[3],
				otherFields[4], otherFields[5], primaryKey[0] };
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
		allFields[Columns.READGLOBALLIMIT.ordinal()].setValue((readgloballimit / 10) * 10);
		allFields[Columns.WRITEGLOBALLIMIT.ordinal()]
				.setValue((writegloballimit / 10) * 10);
		allFields[Columns.READSESSIONLIMIT.ordinal()]
				.setValue((readsessionlimit / 10) * 10);
		allFields[Columns.WRITESESSIONLIMIT.ordinal()]
				.setValue((writesessionlimit / 10) * 10);
		allFields[Columns.DELAYLIMIT.ordinal()].setValue(delayllimit);
		allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
	}

	@Override
	protected void setFromArray() throws WaarpDatabaseSqlException {
		hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
		readgloballimit = (((Long) allFields[Columns.READGLOBALLIMIT.ordinal()]
				.getValue()) / 10) * 10;
		writegloballimit = (((Long) allFields[Columns.WRITEGLOBALLIMIT.ordinal()]
				.getValue()) / 10) * 10;
		readsessionlimit = (((Long) allFields[Columns.READSESSIONLIMIT.ordinal()]
				.getValue()) / 10) * 10;
		writesessionlimit = (((Long) allFields[Columns.WRITESESSIONLIMIT
				.ordinal()].getValue()) / 10) * 10;
		delayllimit = (Long) allFields[Columns.DELAYLIMIT.ordinal()].getValue();
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
	 * @param rg
	 *            Read Global Limit
	 * @param wg
	 *            Write Global Limit
	 * @param rs
	 *            Read Session Limit
	 * @param ws
	 *            Write Session Limit
	 * @param del
	 *            Delay Limit
	 */
	public DbConfiguration(DbSession dbSession, String hostid, long rg, long wg, long rs,
			long ws, long del) {
		super(dbSession);
		this.hostid = hostid;
		readgloballimit = (rg / 10) * 10;
		writegloballimit = (wg / 10) * 10;
		readsessionlimit = (rs / 10) * 10;
		writesessionlimit = (ws / 10) * 10;
		delayllimit = del;
		setToArray();
		isSaved = false;
	}

	/**
	 * @param dbSession
	 * @param hostid
	 * @throws WaarpDatabaseException
	 */
	public DbConfiguration(DbSession dbSession, String hostid) throws WaarpDatabaseException {
		super(dbSession);
		this.hostid = hostid;
		// load from DB
		select();
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.databaseold.data.AbstractDbData#delete()
	 */
	@Override
	public void delete() throws WaarpDatabaseException {
		if (dbSession == null) {
			dbR66ConfigurationHashMap.remove(this.hostid);
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
			dbR66ConfigurationHashMap.put(this.hostid, this);
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
			return dbR66ConfigurationHashMap.containsKey(hostid);
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
			DbConfiguration conf = dbR66ConfigurationHashMap.get(this.hostid);
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
			dbR66ConfigurationHashMap.put(this.hostid, this);
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
	private DbConfiguration(DbSession session) {
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
	public static DbConfiguration getFromStatement(DbPreparedStatement preparedStatement)
			throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
		DbConfiguration dbConfiguration = new DbConfiguration(preparedStatement.getDbSession());
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
	 * Update configuration according to new value of limits
	 */
	public void updateConfiguration() {
		Configuration.configuration.changeNetworkLimit(writegloballimit,
				readgloballimit, writesessionlimit, readsessionlimit, delayllimit);
	}

	/**
	 * 
	 * @return True if this Configuration refers to the current host
	 */
	public boolean isOwnConfiguration() {
		return this.hostid.equals(Configuration.configuration.HOST_ID);
	}
}
