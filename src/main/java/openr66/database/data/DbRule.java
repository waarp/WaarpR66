/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.database.data;

import goldengate.common.file.DirInterface;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.StringReader;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseNoDataException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * Rule Table object
 * @author Frederic Bregier
 *
 */
public class DbRule extends AbstractDbData {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DbRule.class);

    public static enum Columns {
        HOSTIDS,
        MODE,
        RECVPATH,
        SENDPATH,
        ARCHIVEPATH,
        WORKPATH,
        PRETASKS,
        POSTTASKS,
        ERRORTASKS,
        UPDATEDINFO,
        IDRULE
    }

    public static int[] dbTypes = {
            Types.LONGVARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR,
            Types.LONGVARCHAR, Types.INTEGER, Types.VARCHAR };

    public static String table = " RULES ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbRule> dbR66RuleHashMap =
        new ConcurrentHashMap<String, DbRule>();

    /**
     * Internal context XML fields
     */
    public static final String HOSTIDS_HOSTID = "/hostids/hostid";

    /**
     * Internal context XML fields
     */
    private static final String XMLHOSTIDS = "<hostids>";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDHOSTIDS = "</hostids>";

    /**
     * Internal context XML fields
     */
    private static final String XMLHOSTID = "<hostid>";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDHOSTID = "</hostid>";

    /**
     * Internal context XML fields
     */
    public static final String TASKS_ROOT = "/tasks/task";

    /**
     * Internal context XML fields
     */
    private static final String XMLTASKS = "<tasks>";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDTASKS = "</tasks>";

    /**
     * Internal context XML fields
     */
    private static final String XMLTASK = "<task>";

    /**
     * Internal context XML fields
     */
    private static final String XMLENDTASK = "</task>";

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
    public static final String TASK_RANK = "rank";

    /**
     * Global Id
     */
    public String idRule = null;

    /**
     * The Name addresses (serverIds)
     */
    public String ids = null;

    /**
     * Supported Mode for this rule (SENDMODE => SENDMD5MODE, RECVMODE =>
     * RECVMD5MODE)
     */
    public int mode;

    /**
     * The associated Recv Path
     */
    public String recvPath = null;

    /**
     * The associated Send Path
     */
    public String sendPath = null;

    /**
     * The associated Archive Path
     */
    public String archivePath = null;

    /**
     * The associated Work Path
     */
    public String workPath = null;

    /**
     * The associated Pre Tasks
     */
    public String preTasks = null;

    /**
     * The associated Post Tasks
     */
    public String postTasks = null;

    /**
     * The associated Error Tasks
     */
    public String errorTasks = null;

    /**
     * The Ids as an array
     */
    public String[] idsArray = null;

    /**
     * The associated Pre Tasks as an array
     */
    public String[][] preTasksArray = null;

    /**
     * The associated Post Tasks as an array
     */
    public String[][] postTasksArray = null;

    /**
     * The associated Error Tasks as an array
     */
    public String[][] errorTasksArray = null;

    private int updatedInfo = UpdatedInfo.UNKNOWN.ordinal();

    private boolean isSaved = false;

    // ALL TABLE SHOULD IMPLEMENT THIS
    private final DbValue primaryKey = new DbValue(idRule, Columns.IDRULE
            .name());

    private final DbValue[] otherFields = {
            // HOSTIDS, MODE, RECVPATH, SENDPATH, ARCHIVEPATH, WORKPATH,
            // PRETASKS, POSTTASKS, ERRORTASKS
            new DbValue(ids, Columns.HOSTIDS.name(), true),
            new DbValue(mode, Columns.MODE.name()),
            new DbValue(recvPath, Columns.RECVPATH.name()),
            new DbValue(sendPath, Columns.SENDPATH.name()),
            new DbValue(archivePath, Columns.ARCHIVEPATH.name()),
            new DbValue(workPath, Columns.WORKPATH.name()),
            new DbValue(preTasks, Columns.PRETASKS.name(), true),
            new DbValue(postTasks, Columns.POSTTASKS.name(), true),
            new DbValue(errorTasks, Columns.ERRORTASKS.name(), true),
            new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };

    private final DbValue[] allFields = {
            otherFields[0], otherFields[1], otherFields[2], otherFields[3],
            otherFields[4], otherFields[5], otherFields[6], otherFields[7],
            otherFields[8], otherFields[9], primaryKey };

    public static final String selectAllFields = Columns.HOSTIDS.name() + "," +
            Columns.MODE.name() + "," + Columns.RECVPATH.name() + "," +
            Columns.SENDPATH.name() + "," + Columns.ARCHIVEPATH.name() + "," +
            Columns.WORKPATH.name() + "," + Columns.PRETASKS.name() + "," +
            Columns.POSTTASKS.name() + "," + Columns.ERRORTASKS.name() + "," +
            Columns.UPDATEDINFO.name() + "," + Columns.IDRULE.name();

    private static final String updateAllFields = Columns.HOSTIDS.name() +
            "=?," + Columns.MODE.name() + "=?," + Columns.RECVPATH.name() +
            "=?," + Columns.SENDPATH.name() + "=?," +
            Columns.ARCHIVEPATH.name() + "=?," + Columns.WORKPATH.name() +
            "=?," + Columns.PRETASKS.name() + "=?," + Columns.POSTTASKS.name() +
            "=?," + Columns.ERRORTASKS.name() + "=?," +
            Columns.UPDATEDINFO.name() + "=?";

    private static final String insertAllValues = " (?,?,?,?,?,?,?,?,?,?,?) ";

    @Override
    protected void setToArray() {
        allFields[Columns.HOSTIDS.ordinal()].setValue(ids);
        allFields[Columns.MODE.ordinal()].setValue(mode);
        allFields[Columns.RECVPATH.ordinal()].setValue(recvPath);
        allFields[Columns.SENDPATH.ordinal()].setValue(sendPath);
        allFields[Columns.ARCHIVEPATH.ordinal()].setValue(archivePath);
        allFields[Columns.WORKPATH.ordinal()].setValue(workPath);
        allFields[Columns.PRETASKS.ordinal()].setValue(preTasks);
        allFields[Columns.POSTTASKS.ordinal()].setValue(postTasks);
        allFields[Columns.ERRORTASKS.ordinal()].setValue(errorTasks);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
        allFields[Columns.IDRULE.ordinal()].setValue(idRule);
    }

    @Override
    protected void setFromArray() throws OpenR66DatabaseSqlError {
        ids = (String) allFields[Columns.HOSTIDS.ordinal()].getValue();
        mode = (Integer) allFields[Columns.MODE.ordinal()].getValue();
        recvPath = (String) allFields[Columns.RECVPATH.ordinal()].getValue();
        sendPath = (String) allFields[Columns.SENDPATH.ordinal()].getValue();
        archivePath = (String) allFields[Columns.ARCHIVEPATH.ordinal()]
                .getValue();
        workPath = (String) allFields[Columns.WORKPATH.ordinal()].getValue();
        preTasks = (String) allFields[Columns.PRETASKS.ordinal()].getValue();
        postTasks = (String) allFields[Columns.POSTTASKS.ordinal()].getValue();
        errorTasks = (String) allFields[Columns.ERRORTASKS.ordinal()]
                .getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
        idRule = (String) allFields[Columns.IDRULE.ordinal()].getValue();
        getIdsRule(ids);
        preTasksArray = getTasksRule(preTasks);
        postTasksArray = getTasksRule(postTasks);
        errorTasksArray = getTasksRule(errorTasks);
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
     * @param preTasks
     * @param postTasks
     * @param errorTasks
     */
    public DbRule(DbSession dbSession, String idRule, String ids, int mode, String recvPath,
            String sendPath, String archivePath, String workPath,
            String preTasks, String postTasks, String errorTasks) {
        super(dbSession);
        this.idRule = idRule;
        this.ids = ids;
        this.mode = mode;
        this.recvPath = recvPath;
        this.sendPath = sendPath;
        this.archivePath = archivePath;
        this.workPath = workPath;
        this.preTasks = preTasks;
        this.postTasks = postTasks;
        this.errorTasks = errorTasks;
        getIdsRule(this.ids);
        preTasksArray = getTasksRule(this.preTasks);
        postTasksArray = getTasksRule(this.postTasks);
        errorTasksArray = getTasksRule(this.errorTasks);
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param idRule
     * @throws OpenR66DatabaseException
     */
    public DbRule(DbSession dbSession, String idRule) throws OpenR66DatabaseException {
        super(dbSession);
        this.idRule = idRule;
        // load from DB
        select();
        getIdsRule(ids);
        preTasksArray = getTasksRule(preTasks);
        postTasksArray = getTasksRule(postTasks);
        errorTasksArray = getTasksRule(errorTasks);
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
     * @param pretasksArray
     * @param posttasksArray
     * @param errortasksArray
     */
    public DbRule(DbSession dbSession, String idrule, String[] idsArrayRef, int mode,
            String recvpath, String sendpath, String archivepath,
            String workpath, String[][] pretasksArray,
            String[][] posttasksArray, String[][] errortasksArray) {
        super(dbSession);
        idRule = idrule;
        idsArray = idsArrayRef;
        this.mode = mode;
        recvPath = recvpath;
        sendPath = sendpath;
        archivePath = archivepath;
        workPath = workpath;
        preTasksArray = pretasksArray;
        postTasksArray = posttasksArray;
        ids = setIdsRule(idsArrayRef);
        preTasks = setTasksRule(pretasksArray);
        postTasks = setTasksRule(posttasksArray);
        errorTasks = setTasksRule(errortasksArray);
        setToArray();
        isSaved = false;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#delete()
     */
    @Override
    public void delete() throws OpenR66DatabaseException {
        if (dbSession == null) {
            dbR66RuleHashMap.remove(this.idRule);
            isSaved = false;
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("DELETE FROM " + table +
                    " WHERE " + primaryKey.column + " = ?");
            primaryKey.setValue(idRule);
            setValue(preparedStatement, primaryKey);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = false;
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#insert()
     */
    @Override
    public void insert() throws OpenR66DatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66RuleHashMap.put(this.idRule, this);
            isSaved = true;
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("INSERT INTO " + table +
                    " (" + selectAllFields + ") VALUES " + insertAllValues);
            setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }

    /* (non-Javadoc)
     * @see openr66.database.data.AbstractDbData#exist()
     */
    @Override
    public boolean exist() throws OpenR66DatabaseException {
        if (dbSession == null) {
            return dbR66RuleHashMap.containsKey(idRule);
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    primaryKey.column + " FROM " + table + " WHERE " +
                    primaryKey.column + " = ?");
            primaryKey.setValue(idRule);
            setValue(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            return preparedStatement.getNext();
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#select()
     */
    @Override
    public void select() throws OpenR66DatabaseException {
        if (dbSession == null) {
            DbRule rule = dbR66RuleHashMap.get(this.idRule);
            if (rule == null) {
                throw new OpenR66DatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++){
                    allFields[i].value = rule.allFields[i].value;
                }
                setFromArray();
                if (recvPath == null) {
                    recvPath = Configuration.configuration.inPath;
                }
                if (sendPath == null) {
                    sendPath = Configuration.configuration.outPath;
                }
                if (archivePath == null) {
                    archivePath = Configuration.configuration.archivePath;
                }
                if (workPath == null) {
                    workPath = Configuration.configuration.workingPath;
                }
                isSaved = true;
                return;
            }
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("SELECT " +
                    selectAllFields + " FROM " + table + " WHERE " +
                    primaryKey.column + " = ?");
            primaryKey.setValue(idRule);
            setValue(preparedStatement, primaryKey);
            preparedStatement.executeQuery();
            if (preparedStatement.getNext()) {
                getValues(preparedStatement, allFields);
                setFromArray();
                if (recvPath == null) {
                    recvPath = Configuration.configuration.inPath;
                }
                if (sendPath == null) {
                    sendPath = Configuration.configuration.outPath;
                }
                if (archivePath == null) {
                    archivePath = Configuration.configuration.archivePath;
                }
                if (workPath == null) {
                    workPath = Configuration.configuration.workingPath;
                }
                isSaved = true;
            } else {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
        } finally {
            preparedStatement.realClose();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#update()
     */
    @Override
    public void update() throws OpenR66DatabaseNoConnectionError,
            OpenR66DatabaseSqlError, OpenR66DatabaseNoDataException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66RuleHashMap.put(this.idRule, this);
            isSaved = true;
            return;
        }
        DbPreparedStatement preparedStatement = new DbPreparedStatement(
                dbSession);
        try {
            preparedStatement.createPrepareStatement("UPDATE " + table +
                    " SET " + updateAllFields + " WHERE " +
                    primaryKey.column + " = ?");
            setValues(preparedStatement, allFields);
            int count = preparedStatement.executeUpdate();
            if (count <= 0) {
                throw new OpenR66DatabaseNoDataException("No row found");
            }
            isSaved = true;
        } finally {
            preparedStatement.realClose();
        }
    }
    /**
     * Private constructor for Commander only
     */
    private DbRule() {
        super(DbConstant.admin.session);
    }
    /**
     * For instance from Commander when getting updated information
     * @param preparedStatement
     * @return the next updated DbRule
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static DbRule getFromStatement(DbPreparedStatement preparedStatement) throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        DbRule dbRule = new DbRule();
        dbRule.getValues(preparedStatement, dbRule.allFields);
        dbRule.setFromArray();
        dbRule.isSaved = true;
        return dbRule;
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.database.data.AbstractDbData#changeUpdatedInfo(UpdatedInfo)
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
     * Get Ids from String. If it is not ok, then it sets the default values and
     * return False, else returns True.
     *
     * @param idsref
     * @return True if ok, else False (default values).
     */
    @SuppressWarnings("unchecked")
    private boolean getIdsRule(String idsref) {
        if (idsref == null) {
            logger.info("No ids so setting to the default!");
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
            logger.info("No ids so setting to the default!");
            ids = null;
            idsArray = null;
            reader.close();
            return false;
        }
        List<Node> listNode = document.selectNodes(HOSTIDS_HOSTID);
        if (listNode == null) {
            logger
                    .info("Unable to find the id for Rule, setting to the default");
            ids = null;
            idsArray = null;
            reader.close();
            return false;
        }
        idsArray = new String[listNode.size()];
        int i = 0;
        for (Node nodeid: listNode) {
            idsArray[i] = nodeid.getText();
            i ++;
        }
        listNode.clear();
        listNode = null;
        reader.close();
        return true;
    }

    /**
     * Get Tasks from String. If it is not ok, then it sets the default values
     * and return new array of Tasks or null if in error.
     *
     * @param tasks
     * @return Array of tasks or null if in error.
     */
    @SuppressWarnings("unchecked")
    private String[][] getTasksRule(String tasks) {
        if (tasks == null) {
            logger.info("No tasks so setting to the default!");
            return null;
        }
        StringReader reader = new StringReader(tasks);
        Document document = null;
        try {
            document = new SAXReader().read(reader);
        } catch (DocumentException e) {
            logger.warn("Unable to read the tasks for Rule: " + tasks, e);
            logger.info("No tasks so setting to the default!");
            reader.close();
            return null;
        }
        List<Node> listNode = document.selectNodes(TASKS_ROOT);
        if (listNode == null) {
            logger
                    .info("Unable to find the tasks for Rule, setting to the default");
            reader.close();
            return null;
        }
        String[][] taskArray = new String[listNode.size()][3];
        for (int i = 0; i < listNode.size(); i ++) {
            taskArray[i][0] = null;
            taskArray[i][1] = null;
            taskArray[i][2] = null;
        }
        for (Node noderoot: listNode) {
            Node nodetype = null, nodepath = null, noderank = null, nodedelay = null;
            noderank = noderoot.selectSingleNode(TASK_RANK);
            if (noderank == null) {
                continue;
            }
            int rank = Integer.parseInt(noderank.getText());
            nodetype = noderoot.selectSingleNode(TASK_TYPE);
            if (nodetype == null) {
                continue;
            }
            nodepath = noderoot.selectSingleNode(TASK_PATH);
            if (nodepath == null) {
                continue;
            }
            nodedelay = noderoot.selectSingleNode(TASK_DELAY);
            String delay;
            if (nodedelay == null) {
                delay = Integer
                        .toString(Configuration.configuration.TIMEOUTCON);
            } else {
                delay = nodedelay.getText();
            }
            taskArray[rank][0] = nodetype.getText();
            taskArray[rank][1] = nodepath.getText();
            taskArray[rank][2] = delay;
        }
        listNode.clear();
        listNode = null;
        reader.close();
        return taskArray;
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
            for (String element: idsArray) {
                ids += XMLHOSTID + element + XMLENDHOSTID;
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
        String tasks = null;
        if (tasksArray != null) {
            tasks = XMLTASKS;
            for (int i = 0; i < tasksArray.length; i ++) {
                tasks += XMLTASK + "<" + TASK_RANK + ">" + i + "</" +
                        TASK_RANK + "><" + TASK_TYPE + ">" + tasksArray[i][0] +
                        "</" + TASK_TYPE + "><" + TASK_PATH + ">" +
                        tasksArray[i][1] + "</" + TASK_PATH + "><" +
                        TASK_DELAY + ">" + tasksArray[i][2] + "</" +
                        TASK_DELAY + ">" + XMLENDTASK;
            }
            tasks += XMLENDTASKS;
        }
        return tasks;
    }

    /**
     * Get the full path from RecvPath (used only in copy MODE)
     *
     * @param filename
     * @return the full String path
     * @throws OpenR66ProtocolSystemException
     */
    public String setRecvPath(String filename)
            throws OpenR66ProtocolSystemException {
        if (recvPath != null) {
            return recvPath + DirInterface.SEPARATOR + filename;
        }
        return FileUtils.consolidatePath(Configuration.configuration.inPath,
                filename);
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
        return FileUtils.consolidatePath(Configuration.configuration.outPath,
                filename);
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
        return FileUtils.consolidatePath(
                Configuration.configuration.archivePath, filename);
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
        return FileUtils.consolidatePath(
                Configuration.configuration.workingPath, filename);
    }

    /**
     * Check if the given hostTo is in the allowed list
     *
     * @param hostId
     * @return True if allow, else False
     */
    public boolean checkHostAllow(String hostId) {
        if (idsArray == null) {
            return true; // always true in this case
        }
        for (String element: idsArray) {
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
        return "Rule Name:" + idRule + " IDS:" + ids + " MODE: " + mode +
                " RECV:" + recvPath + " SEND:" + sendPath + " ARCHIVE:" +
                archivePath + " WORK:" + workPath + " PRET:" + preTasks +
                " POST:" + postTasks + " ERROR:" + errorTasks;
    }
}
