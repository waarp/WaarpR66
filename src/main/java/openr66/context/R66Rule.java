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
package openr66.context;

import goldengate.common.file.DirInterface;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * @author Frederic Bregier
 *
 */
public class R66Rule {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66Rule.class);

    /**
     * Global HashMap for R66DbRules
     */
    public static final ConcurrentHashMap<String, R66Rule> hashMap = new ConcurrentHashMap<String, R66Rule>();

    /**
     * Global Id
     */
    public String idRule = null;

    /**
     * The Name addresses (serverIds)
     */
    public String ids = null;

    /**
     * The Ids as an array
     */
    public String[] idsArray = null;

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
     * The associated Pre Tasks as an array
     */
    public String[][] preTasksArray = null;

    /**
     * The associated Post Tasks
     */
    public String postTasks = null;

    /**
     * The associated Post Tasks as an array
     */
    public String[][] postTasksArray = null;

    /**
     * The associated Error Tasks
     */
    public String errorTasks = null;

    /**
     * The associated Error Tasks as an array
     */
    public String[][] errorTasksArray = null;

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
    public static final String TASK_RANK = "rank";

    /**
     * Create a Rule from args
     *
     * @param idrule
     * @param ids
     * @param recvpath
     * @param sendpath
     * @param archivepath
     * @param workpath
     * @param pretasks
     * @param posttasks
     * @param errortasks
     */
    public R66Rule(String idrule, String ids, String recvpath, String sendpath,
            String archivepath, String workpath, String pretasks,
            String posttasks, String errortasks) {
        this.assign(idrule, ids, recvpath, sendpath, archivepath, workpath,
                pretasks, posttasks, errortasks);
    }

    /**
     * Create a Rule from args
     *
     * @param idrule
     * @param idsArray
     * @param recvpath
     * @param sendpath
     * @param archivepath
     * @param workpath
     * @param pretasksArray
     * @param posttasksArray
     * @param errortasksArray
     */
    public R66Rule(String idrule, String[] idsArray, String recvpath,
            String sendpath, String archivepath, String workpath,
            String[][] pretasksArray, String[][] posttasksArray,
            String[][] errortasksArray) {
        this.assign(idrule, idsArray, recvpath, sendpath, archivepath,
                workpath, pretasksArray, posttasksArray, errortasksArray);
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
        String[][] taskArray = new String[listNode.size()][2];
        for (int i = 0; i < listNode.size(); i ++) {
            taskArray[i][0] = null;
            taskArray[i][1] = null;
        }
        for (Node noderoot: listNode) {
            Node nodetype = null, nodepath = null, noderank = null;
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
            taskArray[rank][0] = nodetype.getText();
            taskArray[rank][1] = nodepath.getText();
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
                        tasksArray[i][1] + "</" + TASK_PATH + ">" + XMLENDTASK;
            }
            tasks += XMLENDTASKS;
        }
        return tasks;
    }

    /**
     * Assign an Rule from args
     *
     * @param idrule
     * @param idsref
     * @param recvpath
     * @param sendpath
     * @param archivepath
     * @param workpath
     * @param pretasks
     * @param posttasks
     * @param errortasks
     */
    private void assign(String idrule, String idsref, String recvpath,
            String sendpath, String archivepath, String workpath,
            String pretasks, String posttasks, String errortasks) {
        idRule = idrule;
        ids = idsref;
        recvPath = recvpath;
        sendPath = sendpath;
        archivePath = archivepath;
        workPath = workpath;
        preTasks = pretasks;
        postTasks = posttasks;
        getIdsRule(idsref);
        preTasksArray = getTasksRule(pretasks);
        postTasksArray = getTasksRule(posttasks);
        errorTasksArray = getTasksRule(errortasks);
    }

    /**
     * Assign an Rule from args
     *
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
    private void assign(String idrule, String[] idsArrayRef, String recvpath,
            String sendpath, String archivepath, String workpath,
            String[][] pretasksArray, String[][] posttasksArray,
            String[][] errortasksArray) {
        idRule = idrule;
        idsArray = idsArrayRef;
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
    }

    /**
     * Get the Rule from HashMap with rulename
     *
     * @param idrule
     * @return the rule or null if none in HashMap
     */
    private static R66Rule getInternalHash(String idrule) {
        return hashMap.get(idrule);
    }

    /**
     * Add the Rule into HashMap
     *
     * @param rule
     */
    private static void setInternalHash(R66Rule rule) {
        hashMap.put(rule.idRule, rule);
    }

    /**
     * Delete the Rule from HashMap
     *
     * @param idrule
     */
    private static void delInternalHash(String idrule) {
        hashMap.remove(idrule);
    }

    /**
     * Empty the Hashmap of Rules
     */
    public static void emptyRules() {
        hashMap.clear();
    }

    /**
     * Get the R66DbRules object from the Hash
     *
     * @param idrule
     * @return True if OK, else False
     * @throws OpenR66ProtocolNoDataException
     */
    public static R66Rule getHash(String idrule)
            throws OpenR66ProtocolNoDataException {
        R66Rule rule = getInternalHash(idrule);
        if (rule == null) {
            /*
             * DbPreparedStatement p_get_id = setPGet(admin, IDRULE); if
             * (p_get_id == null) return null; ResultSet resultSet =
             * p_get_id.executeQuery(); if
             * (DbPreparedStatement.getNext(resultSet)) { rule = new
             * R66Rule(); if (rule.get(resultSet)) { setInternalHash(rule); }
             * else { rule = null; } } resultSet = null; p_get_id.realClose();
             */
            throw new OpenR66ProtocolNoDataException("Cannot find rule: " +
                    idrule);
        }
        return rule;
    }

    /**
     * Del the Rule object from the Hash
     *
     * @param idrule
     * @return True if OK, else False
     */
    public static boolean del(String idrule) {
        boolean retour = false;
        /*
         * DbPreparedStatement p_del_id = null; p_del_id = setPDel(admin,
         * IDRULE); if (p_del_id == null) return false; if
         * (p_del_id.executeUpdate() == 1) {
         */
        retour = true;
        delInternalHash(idrule);
        /*
         * admin.commit(); } p_del_id.realClose();
         */
        return retour;
    }

    /**
     * Update or Insert data into Hash from object
     *
     * @return True if OK, else False
     */
    public boolean set() {
        boolean retour = false;
        /*
         * DbPreparedStatement p_get_r66 = setPGet(admin, this.idRule); if
         * (p_get_r66 == null) return false; ResultSet resultSet =
         * p_get_r66.executeQuery(); int nb = 0; if
         * (DbPreparedStatement.getNext(resultSet)) { resultSet = null;
         * p_get_r66.realClose(); resultSet = null; DbPreparedStatement
         * p_upd_r66 = setPUpdate(admin, idRule, ids, recvPath, sendPath,
         * archivePath, workPath, preTasks, postTasks, errorTasks); if
         * (p_upd_r66 == null) return false; nb = p_upd_r66.executeUpdate();
         * p_upd_r66.realClose(); if (nb > 0) {
         */
        retour = true;
        /*
         * } } else { resultSet = null; p_get_r66.realClose(); retour =
         * this.insert(admin); }
         */
        if (retour) {
            setInternalHash(this);
        }
        return retour;
    }

    /**
     * Insert data into Hash from object
     *
     * @return True if OK, else False
     */
    public boolean insert() {
        boolean retour = false;
        /*
         * int nb = 0; DbPreparedStatement preparedStatement =
         * setPInsert(admin, idRule, ids, recvPath, sendPath, archivePath,
         * workPath, preTasks, postTasks, errorTasks); if (preparedStatement ==
         * null) { return false; } nb = preparedStatement.executeUpdate();
         * preparedStatement.realClose(); if (nb > 0) {
         */
        retour = true;
        setInternalHash(this);
        /*
         * admin.commit(); }
         */
        return retour;
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
     * Object to String
     *
     * @return the string that displays this object
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Rule Name:" + idRule + " IDS:" + ids + " RECV:" + recvPath +
                " SEND:" + sendPath + " ARCHIVE:" + archivePath + " WORK:" +
                workPath + " PRET:" + preTasks + " POST:" + postTasks +
                " ERROR:" + errorTasks;
    }
}
