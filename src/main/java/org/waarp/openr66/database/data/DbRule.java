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

import java.io.File;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.file.DirInterface;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.common.xml.XmlUtil;
import org.waarp.common.xml.XmlValue;
import org.waarp.openr66.configuration.RuleFileBasedConfiguration;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.database.data.DbTaskRunner.TASKSTEP;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Rule Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbRule extends AbstractDbData {
    /**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(DbRule.class);

    public static enum Columns {
        HOSTIDS,
        MODETRANS,
        RECVPATH,
        SENDPATH,
        ARCHIVEPATH,
        WORKPATH,
        RPRETASKS,
        RPOSTTASKS,
        RERRORTASKS,
        SPRETASKS,
        SPOSTTASKS,
        SERRORTASKS,
        UPDATEDINFO,
        IDRULE
    }

    public static final int[] dbTypes = {
            Types.LONGVARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR,
            Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR,
            Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR,
            Types.INTEGER, Types.NVARCHAR };

    public static final String table = " RULES ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbRule> dbR66RuleHashMap =
            new ConcurrentHashMap<String, DbRule>();

    /**
     * Internal context XML fields
     */
    private static final String XMLHOSTIDS = "<"
            + RuleFileBasedConfiguration.XHOSTIDS
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDHOSTIDS = "</"
            + RuleFileBasedConfiguration.XHOSTIDS
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLHOSTID = "<"
            + RuleFileBasedConfiguration.XHOSTID
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDHOSTID = "</"
            + RuleFileBasedConfiguration.XHOSTID
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLTASKS = "<"
            + RuleFileBasedConfiguration.XTASKS
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDTASKS = "</"
            + RuleFileBasedConfiguration.XTASKS
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLTASK = "<"
            + RuleFileBasedConfiguration.XTASK
            + ">";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDTASK = "</"
            + RuleFileBasedConfiguration.XTASK
            + ">";

    /**
     * Internal context XML fields
     */
    public static final String TASK_TYPE = "type";

    /**
     * Internal context XML fields
     */
    public static final String TASK_PATH = "path";

    /**
     * Internal context XML fields
     */
    public static final String TASK_DELAY = "delay";
    /**
     * Internal context XML fields
     */
    public static final String TASK_COMMENT = "comment";

    /**
     * Global Id
     */
    public String idRule = null;

    /**
     * The Name addresses (serverIds)
     */
    public String ids = null;

    /**
     * Supported Mode for this rule (SENDMODE => SENDMD5MODE, RECVMODE => RECVMD5MODE)
     */
    public int mode;

    /**
     * The associated Recv Path
     */
    private String recvPath = null;

    /**
     * The associated Send Path
     */
    private String sendPath = null;

    /**
     * The associated Archive Path
     */
    private String archivePath = null;

    /**
     * The associated Work Path
     */
    private String workPath = null;

    /**
     * The associated Pre Tasks for Receiver
     */
    public String rpreTasks = null;

    /**
     * The associated Post Tasks for Receiver
     */
    public String rpostTasks = null;

    /**
     * The associated Error Tasks for Receiver
     */
    public String rerrorTasks = null;

    /**
     * The associated Pre Tasks for Sender
     */
    public String spreTasks = null;

    /**
     * The associated Post Tasks for Sender
     */
    public String spostTasks = null;

    /**
     * The associated Error Tasks for Sender
     */
    public String serrorTasks = null;

    /**
     * The Ids as an array
     */
    public String[] idsArray = null;

    /**
     * The associated Pre Tasks as an array for Receiver
     */
    public String[][] rpreTasksArray = null;

    /**
     * The associated Post Tasks as an array for Receiver
     */
    public String[][] rpostTasksArray = null;

    /**
     * The associated Error Tasks as an array for Receiver
     */
    public String[][] rerrorTasksArray = null;

    /**
     * The associated Pre Tasks as an array for Sender
     */
    public String[][] spreTasksArray = null;

    /**
     * The associated Post Tasks as an array for Sender
     */
    public String[][] spostTasksArray = null;

    /**
     * The associated Error Tasks as an array for Sender
     */
    public String[][] serrorTasksArray = null;

    private int updatedInfo = UpdatedInfo.UNKNOWN
            .ordinal();

    // ALL TABLE SHOULD IMPLEMENT THIS
    public static final int NBPRKEY = 1;

    protected static final String selectAllFields = Columns.HOSTIDS
            .name()
            + ","
            +
            Columns.MODETRANS
                    .name()
            + ","
            + Columns.RECVPATH
                    .name()
            + ","
            +
            Columns.SENDPATH
                    .name()
            + ","
            + Columns.ARCHIVEPATH
                    .name()
            + ","
            +
            Columns.WORKPATH
                    .name()
            + ","
            +
            Columns.RPRETASKS
                    .name()
            + ","
            +
            Columns.RPOSTTASKS
                    .name()
            + ","
            + Columns.RERRORTASKS
                    .name()
            + ","
            +
            Columns.SPRETASKS
                    .name()
            + ","
            +
            Columns.SPOSTTASKS
                    .name()
            + ","
            + Columns.SERRORTASKS
                    .name()
            + ","
            +
            Columns.UPDATEDINFO
                    .name()
            + ","
            + Columns.IDRULE
                    .name();

    protected static final String updateAllFields = Columns.HOSTIDS
            .name()
            +
            "=?,"
            + Columns.MODETRANS
                    .name()
            + "=?,"
            + Columns.RECVPATH
                    .name()
            +
            "=?,"
            + Columns.SENDPATH
                    .name()
            + "=?,"
            +
            Columns.ARCHIVEPATH
                    .name()
            + "=?,"
            + Columns.WORKPATH
                    .name()
            +
            "=?,"
            + Columns.RPRETASKS
                    .name()
            + "=?,"
            + Columns.RPOSTTASKS
                    .name()
            +
            "=?,"
            + Columns.RERRORTASKS
                    .name()
            + "=?,"
            +
            Columns.SPRETASKS
                    .name()
            + "=?,"
            + Columns.SPOSTTASKS
                    .name()
            +
            "=?,"
            + Columns.SERRORTASKS
                    .name()
            + "=?,"
            +
            Columns.UPDATEDINFO
                    .name()
            + "=?";

    protected static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

    /*
     * (non-Javadoc)
     * @see org.waarp.common.database.data.AbstractDbData#initObject()
     */
    @Override
    protected void initObject() {
        primaryKey = new DbValue[] { new DbValue(idRule, Columns.IDRULE
                .name()) };
        otherFields = new DbValue[] {
                // HOSTIDS, MODETRANS, RECVPATH, SENDPATH, ARCHIVEPATH, WORKPATH,
                // PRETASKS, POSTTASKS, ERRORTASKS
                new DbValue(ids, Columns.HOSTIDS.name(), true),
                new DbValue(mode, Columns.MODETRANS.name()),
                new DbValue(recvPath, Columns.RECVPATH.name()),
                new DbValue(sendPath, Columns.SENDPATH.name()),
                new DbValue(archivePath, Columns.ARCHIVEPATH.name()),
                new DbValue(workPath, Columns.WORKPATH.name()),
                new DbValue(rpreTasks, Columns.RPRETASKS.name(), true),
                new DbValue(rpostTasks, Columns.RPOSTTASKS.name(), true),
                new DbValue(rerrorTasks, Columns.RERRORTASKS.name(), true),
                new DbValue(spreTasks, Columns.SPRETASKS.name(), true),
                new DbValue(spostTasks, Columns.SPOSTTASKS.name(), true),
                new DbValue(serrorTasks, Columns.SERRORTASKS.name(), true),
                new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };
        allFields = new DbValue[] {
                otherFields[0], otherFields[1], otherFields[2], otherFields[3],
                otherFields[4], otherFields[5], otherFields[6], otherFields[7],
                otherFields[8], otherFields[9], otherFields[10],
                otherFields[11], otherFields[12], primaryKey[0] };
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

    protected final void checkPath() {
        if (recvPath != null && !recvPath.isEmpty() && recvPath.charAt(0) != DirInterface.SEPARATORCHAR) {
            recvPath = DirInterface.SEPARATOR + recvPath;
        }
        if (sendPath != null && !sendPath.isEmpty() && sendPath.charAt(0) != DirInterface.SEPARATORCHAR) {
            sendPath = DirInterface.SEPARATOR + sendPath;
        }
        if (archivePath != null && !archivePath.isEmpty() && archivePath.charAt(0) != DirInterface.SEPARATORCHAR) {
            archivePath = DirInterface.SEPARATOR + archivePath;
        }
        if (workPath != null && !workPath.isEmpty() && workPath.charAt(0) != DirInterface.SEPARATORCHAR) {
            workPath = DirInterface.SEPARATOR + workPath;
        }
    }

    @Override
    protected void setToArray() {
        checkPath();
        allFields[Columns.HOSTIDS.ordinal()].setValue(ids);
        allFields[Columns.MODETRANS.ordinal()].setValue(mode);
        allFields[Columns.RECVPATH.ordinal()].setValue(recvPath);
        allFields[Columns.SENDPATH.ordinal()].setValue(sendPath);
        allFields[Columns.ARCHIVEPATH.ordinal()].setValue(archivePath);
        allFields[Columns.WORKPATH.ordinal()].setValue(workPath);
        allFields[Columns.RPRETASKS.ordinal()].setValue(rpreTasks);
        allFields[Columns.RPOSTTASKS.ordinal()].setValue(rpostTasks);
        allFields[Columns.RERRORTASKS.ordinal()].setValue(rerrorTasks);
        allFields[Columns.SPRETASKS.ordinal()].setValue(spreTasks);
        allFields[Columns.SPOSTTASKS.ordinal()].setValue(spostTasks);
        allFields[Columns.SERRORTASKS.ordinal()].setValue(serrorTasks);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.IDRULE.ordinal()].setValue(idRule);
    }

    @Override
    protected void setFromArray() throws WaarpDatabaseSqlException {
        ids = (String) allFields[Columns.HOSTIDS.ordinal()].getValue();
        mode = (Integer) allFields[Columns.MODETRANS.ordinal()].getValue();
        recvPath = (String) allFields[Columns.RECVPATH.ordinal()].getValue();
        sendPath = (String) allFields[Columns.SENDPATH.ordinal()].getValue();
        archivePath = (String) allFields[Columns.ARCHIVEPATH.ordinal()]
                .getValue();
        workPath = (String) allFields[Columns.WORKPATH.ordinal()].getValue();
        rpreTasks = (String) allFields[Columns.RPRETASKS.ordinal()].getValue();
        rpostTasks = (String) allFields[Columns.RPOSTTASKS.ordinal()].getValue();
        rerrorTasks = (String) allFields[Columns.RERRORTASKS.ordinal()]
                .getValue();
        spreTasks = (String) allFields[Columns.SPRETASKS.ordinal()].getValue();
        spostTasks = (String) allFields[Columns.SPOSTTASKS.ordinal()].getValue();
        serrorTasks = (String) allFields[Columns.SERRORTASKS.ordinal()]
                .getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        idRule = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        getIdsRule(ids);
        rpreTasksArray = getTasksRule(rpreTasks);
        rpostTasksArray = getTasksRule(rpostTasks);
        rerrorTasksArray = getTasksRule(rerrorTasks);
        spreTasksArray = getTasksRule(spreTasks);
        spostTasksArray = getTasksRule(spostTasks);
        serrorTasksArray = getTasksRule(serrorTasks);
        checkPath();
    }

    protected void setFromArrayClone(DbRule source) throws WaarpDatabaseSqlException {
        ids = (String) allFields[Columns.HOSTIDS.ordinal()].getValue();
        mode = (Integer) allFields[Columns.MODETRANS.ordinal()].getValue();
        recvPath = (String) allFields[Columns.RECVPATH.ordinal()].getValue();
        sendPath = (String) allFields[Columns.SENDPATH.ordinal()].getValue();
        archivePath = (String) allFields[Columns.ARCHIVEPATH.ordinal()]
                .getValue();
        workPath = (String) allFields[Columns.WORKPATH.ordinal()].getValue();
        rpreTasks = (String) allFields[Columns.RPRETASKS.ordinal()].getValue();
        rpostTasks = (String) allFields[Columns.RPOSTTASKS.ordinal()].getValue();
        rerrorTasks = (String) allFields[Columns.RERRORTASKS.ordinal()]
                .getValue();
        spreTasks = (String) allFields[Columns.SPRETASKS.ordinal()].getValue();
        spostTasks = (String) allFields[Columns.SPOSTTASKS.ordinal()].getValue();
        serrorTasks = (String) allFields[Columns.SERRORTASKS.ordinal()]
                .getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        idRule = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        if (source.ids == null) {
            // No ids so setting to the default!
            ids = null;
            idsArray = null;
        } else {
            ids = source.ids;
            idsArray = source.idsArray;
        }
        rpreTasksArray = source.rpreTasksArray;
        rpostTasksArray = source.rpostTasksArray;
        rerrorTasksArray = source.rerrorTasksArray;
        spreTasksArray = source.spreTasksArray;
        spostTasksArray = source.spostTasksArray;
        serrorTasksArray = source.serrorTasksArray;
        checkPath();
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
        primaryKey[0].setValue(idRule);
    }

    /**
     * @param dbSession
     * @param idRule
     * @param ids
     * @param mode
     * @param recvPath
     * @param sendPath
     * @param archivePath
     * @param workPath
     * @param rpreTasks
     * @param rpostTasks
     * @param rerrorTasks
     * @param spreTasks
     * @param spostTasks
     * @param serrorTasks
     */
    public DbRule(DbSession dbSession, String idRule, String ids, int mode, String recvPath,
            String sendPath, String archivePath, String workPath,
            String rpreTasks, String rpostTasks, String rerrorTasks,
            String spreTasks, String spostTasks, String serrorTasks) {
        super(dbSession);
        this.idRule = idRule;
        this.ids = ids;
        this.mode = mode;
        this.recvPath = recvPath;
        this.sendPath = sendPath;
        this.archivePath = archivePath;
        this.workPath = workPath;
        this.rpreTasks = rpreTasks;
        this.rpostTasks = rpostTasks;
        this.rerrorTasks = rerrorTasks;
        this.spreTasks = spreTasks;
        this.spostTasks = spostTasks;
        this.serrorTasks = serrorTasks;
        getIdsRule(this.ids);
        rpreTasksArray = getTasksRule(this.rpreTasks);
        rpostTasksArray = getTasksRule(this.rpostTasks);
        rerrorTasksArray = getTasksRule(this.rerrorTasks);
        spreTasksArray = getTasksRule(this.spreTasks);
        spostTasksArray = getTasksRule(this.spostTasks);
        serrorTasksArray = getTasksRule(this.serrorTasks);
        // and reverse
        this.rpreTasks = setTasksRule(rpreTasksArray);
        this.rpostTasks = setTasksRule(rpostTasksArray);
        this.rerrorTasks = setTasksRule(rerrorTasksArray);
        this.spreTasks = setTasksRule(spreTasksArray);
        this.spostTasks = setTasksRule(spostTasksArray);
        this.serrorTasks = setTasksRule(serrorTasksArray);
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param idRule
     * @throws WaarpDatabaseException
     */
    public DbRule(DbSession dbSession, String idRule) throws WaarpDatabaseException {
        super(dbSession);
        this.idRule = idRule;
        // load from DB
        select();
    }

    /**
     * Constructor used from XML file
     * 
     * @param dbSession
     * @param idrule
     * @param idsArrayRef
     * @param recvpath
     * @param sendpath
     * @param archivepath
     * @param workpath
     * @param rpretasksArray
     * @param rposttasksArray
     * @param rerrortasksArray
     * @param spretasksArray
     * @param sposttasksArray
     * @param serrortasksArray
     */
    public DbRule(DbSession dbSession, String idrule, String[] idsArrayRef, int mode,
            String recvpath, String sendpath, String archivepath,
            String workpath,
            String[][] rpretasksArray, String[][] rposttasksArray, String[][] rerrortasksArray,
            String[][] spretasksArray, String[][] sposttasksArray, String[][] serrortasksArray) {
        super(dbSession);
        idRule = idrule;
        idsArray = idsArrayRef;
        this.mode = mode;
        recvPath = recvpath;
        sendPath = sendpath;
        archivePath = archivepath;
        workPath = workpath;
        rpreTasksArray = rpretasksArray;
        rpostTasksArray = rposttasksArray;
        rerrorTasksArray = rerrortasksArray;
        spreTasksArray = spretasksArray;
        spostTasksArray = sposttasksArray;
        serrorTasksArray = serrortasksArray;
        ids = setIdsRule(idsArrayRef);
        rpreTasks = setTasksRule(rpretasksArray);
        rpostTasks = setTasksRule(rposttasksArray);
        rerrorTasks = setTasksRule(rerrortasksArray);
        spreTasks = setTasksRule(spretasksArray);
        spostTasks = setTasksRule(sposttasksArray);
        serrorTasks = setTasksRule(serrortasksArray);
        setToArray();
        isSaved = false;
    }

    /**
     * Constructor from Json
     * 
     * @param dbSession
     * @param source
     * @throws WaarpDatabaseSqlException
     */
    public DbRule(DbSession dbSession, ObjectNode source) throws WaarpDatabaseSqlException {
        super(dbSession);
        setFromJson(source, false);
        if (idRule == null || idRule.isEmpty()) {
            throw new WaarpDatabaseSqlException("Not enough argument to create the object");
        }
    }

    @Override
    public void setFromJson(ObjectNode node, boolean ignorePrimaryKey) throws WaarpDatabaseSqlException {
        super.setFromJson(node, ignorePrimaryKey);
        getIdsRule(this.ids);
        rpreTasksArray = getTasksRule(this.rpreTasks);
        rpostTasksArray = getTasksRule(this.rpostTasks);
        rerrorTasksArray = getTasksRule(this.rerrorTasks);
        spreTasksArray = getTasksRule(this.spreTasks);
        spostTasksArray = getTasksRule(this.spostTasks);
        serrorTasksArray = getTasksRule(this.serrorTasks);
        // and reverse
        this.rpreTasks = setTasksRule(rpreTasksArray);
        this.rpostTasks = setTasksRule(rpostTasksArray);
        this.rerrorTasks = setTasksRule(rerrorTasksArray);
        this.spreTasks = setTasksRule(spreTasksArray);
        this.spostTasks = setTasksRule(spostTasksArray);
        this.serrorTasks = setTasksRule(serrorTasksArray);
        setToArray();
        isSaved = false;
    }

    /**
     * Delete all entries (used when purge and reload)
     * 
     * @param dbSession
     * @return the previous existing array of DbRule
     * @throws WaarpDatabaseException
     */
    public static DbRule[] deleteAll(DbSession dbSession) throws WaarpDatabaseException {
        DbRule[] result = getAllRules(dbSession);
        dbR66RuleHashMap.clear();
        if (dbSession == null) {
            return result;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table);
            preparedStatement.executeUpdate();
            return result;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.waarp.openr66.databaseold.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws WaarpDatabaseException {
        dbR66RuleHashMap.remove(this.idRule);
        if (dbSession == null) {
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
        dbR66RuleHashMap.put(this.idRule, this);
        if (dbSession == null) {
            isSaved = true;
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
        boolean result = dbR66RuleHashMap.containsKey(idRule);
        if (result) {
            return result;
        }
        if (dbSession == null) {
            if (!result) {
                isSaved = false;
            }
            return result;
        }
        return super.exist();
    }

    /*
     * (non-Javadoc)
     * @see org.waarp.openr66.databaseold.data.AbstractDbData#select()
     */
    @Override
    public void select() throws WaarpDatabaseException {
        DbRule rule = dbR66RuleHashMap.get(this.idRule);
        if (rule != null) {
            // copy info
            for (int i = 0; i < allFields.length; i++) {
                allFields[i].value = rule.allFields[i].value;
            }
            setFromArrayClone(rule);
            if (recvPath == null || recvPath.trim().isEmpty()) {
                recvPath = "";
            }
            if (sendPath == null || sendPath.trim().isEmpty()) {
                sendPath = "";
            }
            if (archivePath == null || archivePath.trim().isEmpty()) {
                archivePath = "";
            }
            if (workPath == null || workPath.trim().isEmpty()) {
                workPath = "";
            }
            isSaved = true;
            return;
        }
        if (dbSession == null) {
            if (rule == null) {
                throw new WaarpDatabaseNoDataException("No row found");
            }
        }
        super.select();
        if (recvPath == null || recvPath.trim().isEmpty()) {
            recvPath = "";
        }
        if (sendPath == null || sendPath.trim().isEmpty()) {
            sendPath = "";
        }
        if (archivePath == null || archivePath.trim().isEmpty()) {
            archivePath = "";
        }
        if (workPath == null || workPath.trim().isEmpty()) {
            workPath = "";
        }
        setFromArray();
        dbR66RuleHashMap.put(this.idRule, this);
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
        dbR66RuleHashMap.put(this.idRule, this);
        if (dbSession == null) {
            isSaved = true;
            return;
        }
        super.update();
    }

    /**
     * Private constructor for Commander only
     */
    private DbRule(DbSession session) {
        super(session);
    }

    /**
     * Get All DbRule from database or from internal hashMap in case of no database support
     * 
     * @param dbSession
     *            may be null
     * @return the array of DbRule
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbRule[] getAllRules(DbSession dbSession)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbRule[] result = new DbRule[0];
        if (!dbR66RuleHashMap.isEmpty()) {
            return dbR66RuleHashMap.values().toArray(result);
        }
        if (dbSession == null) {
            return result;
        }
        String request = "SELECT " + selectAllFields;
        request += " FROM " + table;
        DbPreparedStatement preparedStatement = new DbPreparedStatement(dbSession, request);
        ArrayList<DbRule> dbArrayList = new ArrayList<DbRule>();
        preparedStatement.executeQuery();
        while (preparedStatement.getNext()) {
            DbRule dbrule = getFromStatement(preparedStatement);
            dbArrayList.add(dbrule);
        }
        preparedStatement.realClose();
        return dbArrayList.toArray(result);
    }

    /**
     * For instance from Commander when getting updated information
     * 
     * @param preparedStatement
     * @return the next updated DbRule
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbRule getFromStatement(DbPreparedStatement preparedStatement)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbRule dbRule = new DbRule(preparedStatement.getDbSession());
        dbRule.getValues(preparedStatement, dbRule.allFields);
        dbRule.setFromArray();
        dbRule.isSaved = true;
        logger.debug("Get one Rule from Db: " + dbRule.idRule);
        dbR66RuleHashMap.put(dbRule.idRule, dbRule);
        return dbRule;
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
                AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal();
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
     * Get Ids from String. If it is not ok, then it sets the default values and return False, else
     * returns True.
     * 
     * @param idsref
     * @return True if ok, else False (default values).
     */
    private boolean getIdsRule(String idsref) {
        if (idsref == null) {
            // No ids so setting to the default!
            ids = null;
            idsArray = null;
            return false;
        }
        ids = idsref;
        StringReader reader = new StringReader(idsref);
        Document document = null;
        try {
            document = new SAXReader().read(reader);
        } catch (DocumentException e) {
            logger.warn("Unable to read the ids for Rule: " + idsref, e);
            // No ids so setting to the default!
            ids = null;
            idsArray = null;
            reader.close();
            return false;
        }
        XmlValue[] values = XmlUtil.read(document,
                RuleFileBasedConfiguration.hostsDecls);
        idsArray = RuleFileBasedConfiguration.getHostIds(values[0]);
        reader.close();
        return true;
    }

    /**
     * Get Tasks from String. If it is not ok, then it sets the default values and return new array
     * of Tasks or null if in error.
     * 
     * @param tasks
     * @return Array of tasks or empty array if in error.
     */
    private String[][] getTasksRule(String tasks) {
        if (tasks == null) {
            // No tasks so setting to the default!
            return new String[0][0];
        }
        StringReader reader = new StringReader(tasks);
        Document document = null;
        try {
            document = new SAXReader().read(reader);
        } catch (DocumentException e) {
            logger.info("Unable to read the tasks for Rule: " + tasks, e);
            // No tasks so setting to the default!
            reader.close();
            return new String[0][0];
        }
        XmlValue[] values = XmlUtil.read(document, RuleFileBasedConfiguration.tasksDecl);
        String[][] result = RuleFileBasedConfiguration.getTasksRule(values[0]);
        reader.close();
        return result;
    }

    /**
     * Initialized a ids String from args
     * 
     * @param idsArray
     * @return the new ids string
     */
    private static String setIdsRule(String[] idsArray) {
        String ids = null;
        if (idsArray != null) {
            ids = XMLHOSTIDS;
            for (String element : idsArray) {
                ids += XMLHOSTID + element + XMLENDHOSTID + "\n";
            }
            ids += XMLENDHOSTIDS;
        }
        return ids;
    }

    /**
     * Initialized a tasks String from args
     * 
     * @param tasksArray
     * @return the new tasks string
     */
    private static String setTasksRule(String[][] tasksArray) {
        StringBuilder builder = new StringBuilder();
        if (tasksArray != null) {
            builder.append(XMLTASKS);
            for (int i = 0; i < tasksArray.length; i++) {
                builder.append(XMLTASK)
                        .append('<').append(TASK_TYPE).append('>')
                        .append(tasksArray[i][0])
                        .append("</").append(TASK_TYPE).append(">\n")
                        .append('<').append(TASK_PATH).append('>')
                        .append(tasksArray[i][1])
                        .append("</").append(TASK_PATH).append(">\n")
                        .append('<').append(TASK_DELAY).append('>')
                        .append(tasksArray[i][2])
                        .append("</").append(TASK_DELAY).append('>');
                if (tasksArray[i].length > 3) {
                    builder.append('<').append(TASK_COMMENT).append('>')
                            .append(tasksArray[i][3])
                            .append("</").append(TASK_COMMENT).append('>');
                }
                builder.append(XMLENDTASK).append('\n');
            }
            builder.append(XMLENDTASKS);
        }
        logger.debug("RuleTask: {}", builder);
        return builder.toString();
    }

    /**
     * Get the full path from RecvPath (used only in copy MODETRANS)
     * 
     * @param filename
     * @return the full String path
     * @throws OpenR66ProtocolSystemException
     */
    public String setRecvPath(String filename)
            throws OpenR66ProtocolSystemException {
        if (recvPath != null && !recvPath.isEmpty()) {
            return recvPath + DirInterface.SEPARATOR + filename;
        }
        return Configuration.configuration.inPath + DirInterface.SEPARATOR + filename;
    }

    /**
     * Get the full path from sendPath
     * 
     * @param filename
     * @return the full String path
     * @throws OpenR66ProtocolSystemException
     */
    public String setSendPath(String filename)
            throws OpenR66ProtocolSystemException {
        if (sendPath != null) {
            File file = new File(filename);
            String basename = file.getName();
            return sendPath + DirInterface.SEPARATOR + basename;
        }
        return Configuration.configuration.outPath + DirInterface.SEPARATOR + filename;
    }

    /**
     * Get the full path from archivePath
     * 
     * @param filename
     * @return the full String path
     * @throws OpenR66ProtocolSystemException
     */
    public String setArchivePath(String filename)
            throws OpenR66ProtocolSystemException {
        if (archivePath != null) {
            return archivePath + DirInterface.SEPARATOR + filename;
        }
        return Configuration.configuration.archivePath + DirInterface.SEPARATOR + filename;
    }

    /**
     * Get the full path from workPath
     * 
     * @param filename
     * @return the full String path
     * @throws OpenR66ProtocolSystemException
     */
    public String setWorkingPath(String filename)
            throws OpenR66ProtocolSystemException {
        if (workPath != null) {
            return workPath + DirInterface.SEPARATOR + filename +
                    Configuration.EXT_R66;
        }
        return Configuration.configuration.workingPath + DirInterface.SEPARATOR + filename;
    }

    /**
     * Check if the given hostTo is in the allowed list
     * 
     * @param hostId
     * @return True if allow, else False
     */
    public boolean checkHostAllow(String hostId) {
        if (idsArray == null || idsArray.length == 0) {
            return true; // always true in this case
        }
        for (String element : idsArray) {
            if (element.equalsIgnoreCase(hostId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @return True if this rule is adapted for SENDMODE
     */
    public boolean isSendMode() {
        return (!RequestPacket.isRecvMode(mode));
    }

    /**
     * 
     * @return True if this rule is adapted for RECVMODE
     */
    public boolean isRecvMode() {
        return RequestPacket.isRecvMode(mode);
    }

    /**
     * Object to String
     * 
     * @return the string that displays this object
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Rule Name:" + idRule + " IDS:" + ids + " MODETRANS: " +
                RequestPacket.TRANSFERMODE.values()[mode].toString() +
                " RECV:" + getRecvPath() + " SEND:" + getSendPath() + " ARCHIVE:" +
                getArchivePath() + " WORK:" + getWorkPath() +
                " RPRET:{" + rpreTasks.replace('\n', ' ') + "} RPOST:{" + rpostTasks.replace('\n', ' ') + "} RERROR:{"
                + rerrorTasks.replace('\n', ' ') +
                "} SPRET:{" + spreTasks.replace('\n', ' ') + "} SPOST:{" + spostTasks.replace('\n', ' ') + "} SERROR:{"
                + serrorTasks.replace('\n', ' ') + "}";
    }

    /**
     * 
     * @param isSender
     * @param step
     * @return a string that prints (debug) the tasks to execute
     */
    public String printTasks(boolean isSender, TASKSTEP step) {
        if (isSender) {
            switch (step) {
                case PRETASK:
                    return "S:{" + spreTasks.replace('\n', ' ') + "}";
                case POSTTASK:
                    return "S:{" + spostTasks.replace('\n', ' ') + "}";
                case ERRORTASK:
                    return "S:{" + serrorTasks.replace('\n', ' ') + "}";
                default:
                    return "S:{no task}";
            }
        } else {
            switch (step) {
                case PRETASK:
                    return "R:{" + rpreTasks.replace('\n', ' ') + "}";
                case POSTTASK:
                    return "R:{" + rpostTasks.replace('\n', ' ') + "}";
                case ERRORTASK:
                    return "R:{" + rerrorTasks.replace('\n', ' ') + "}";
                default:
                    return "R:{no task}";
            }
        }
    }

    /**
     * Object to String
     * 
     * @return the string that displays this object
     * @see java.lang.Object#toString()
     */
    public String toShortString() {
        return "Rule Name:" + idRule + " MODETRANS: " +
                RequestPacket.TRANSFERMODE.values()[mode].toString();
    }

    /**
     * 
     * @param session
     * @param rule
     * @param mode
     * @return the DbPreparedStatement according to the filter
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getFilterPrepareStament(DbSession session,
            String rule, int mode)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table;
        String condition = null;
        if (rule != null) {
            condition = " WHERE " + Columns.IDRULE.name() + " LIKE '%" + rule + "%' ";
        }
        if (mode >= 0) {
            if (condition != null) {
                condition += " AND ";
            } else {
                condition = " WHERE ";
            }
            condition += Columns.MODETRANS.name() + " = ?";
        } else {
            condition = "";
        }
        preparedStatement.createPrepareStatement(request + condition +
                " ORDER BY " + Columns.IDRULE.name());
        if (mode >= 0) {
            try {
                preparedStatement.getPreparedStatement().setInt(1, mode);
            } catch (SQLException e) {
                preparedStatement.realClose();
                throw new WaarpDatabaseSqlException(e);
            }
        }
        return preparedStatement;
    }

    /**
     * @param session
     * @param body
     * @return the runner in Html format specified by body by replacing all instance of fields
     */
    public String toSpecializedHtml(R66Session session, String body) {
        StringBuilder builder = new StringBuilder(body);
        WaarpStringUtils.replace(builder, "XXXRULEXXX", idRule);
        WaarpStringUtils.replace(builder, "XXXIDSXXX", ids == null ? "" : ids);
        if (mode == RequestPacket.TRANSFERMODE.RECVMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXRECVXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.SENDMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXSENDXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.RECVMD5MODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXRECVMXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.SENDMD5MODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXSENDMXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.RECVTHROUGHMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXRECVTXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.SENDTHROUGHMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXSENDTXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.RECVMD5THROUGHMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXRECVMTXXX", "checked");
        } else if (mode == RequestPacket.TRANSFERMODE.SENDMD5THROUGHMODE.ordinal()) {
            WaarpStringUtils.replace(builder, "XXXSENDMTXXX", "checked");
        }
        WaarpStringUtils.replace(builder, "XXXRPXXX", recvPath == null ? "" : recvPath);
        WaarpStringUtils.replace(builder, "XXXSPXXX", sendPath == null ? "" : sendPath);
        WaarpStringUtils.replace(builder, "XXXAPXXX", archivePath == null ? "" : archivePath);
        WaarpStringUtils.replace(builder, "XXXWPXXX", workPath == null ? "" : workPath);
        WaarpStringUtils.replace(builder, "XXXRPTXXX", rpreTasks == null ? "" : rpreTasks);
        WaarpStringUtils.replace(builder, "XXXRSTXXX", rpostTasks == null ? "" : rpostTasks);
        WaarpStringUtils.replace(builder, "XXXRETXXX", rerrorTasks == null ? "" : rerrorTasks);
        WaarpStringUtils.replace(builder, "XXXSPTXXX", spreTasks == null ? "" : spreTasks);
        WaarpStringUtils.replace(builder, "XXXSSTXXX", spostTasks == null ? "" : spostTasks);
        WaarpStringUtils.replace(builder, "XXXSETXXX", serrorTasks == null ? "" : serrorTasks);
        return builder.toString();
    }

    /**
     * @return the recvPath
     */
    public String getRecvPath() {
        if (recvPath == null || recvPath.trim().isEmpty())
            return Configuration.configuration.inPath;
        return recvPath;
    }

    /**
     * @return the sendPath
     */
    public String getSendPath() {
        if (sendPath == null || sendPath.trim().isEmpty())
            return Configuration.configuration.outPath;
        return sendPath;
    }

    /**
     * @return the archivePath
     */
    public String getArchivePath() {
        if (archivePath == null || archivePath.trim().isEmpty())
            return Configuration.configuration.archivePath;
        return archivePath;
    }

    /**
     * @return the workPath
     */
    public String getWorkPath() {
        if (workPath == null || workPath.trim().isEmpty())
            return Configuration.configuration.workingPath;
        return workPath;
    }

    /**
     * @return the Rule recvPath
     */
    public String getRuleRecvPath() {
        return recvPath;
    }

    /**
     * @return the Rule sendPath
     */
    public String getRuleSendPath() {
        return sendPath;
    }

    /**
     * @return the Rule archivePath
     */
    public String getRuleArchivePath() {
        return archivePath;
    }

    /**
     * @return the Rule workPath
     */
    public String getRuleWorkPath() {
        return workPath;
    }

    /**
     * 
     * @return the DbValue associated with this table
     */
    public static DbValue[] getAllType() {
        DbRule item = new DbRule(null);
        return item.allFields;
    }
}
