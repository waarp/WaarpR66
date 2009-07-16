/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.config;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import openr66.filesystem.R66Dir;
import openr66.filesystem.R66Rule;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

/**
 * @author Frederic Bregier
 *
 */
public class R66RuleFileBasedConfiguration {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66RuleFileBasedConfiguration.class);

    private static final String IDRULE = "/rule/idrule";
    private static final String HOSTIDS_HOSTID = "/rule/hostids/hostid";
    private static final String RECVPATH = "/rule/recvpath";
    private static final String SENDPATH = "/rule/sendpath";
    private static final String ARCHIVEPATH = "/rule/archivepath";
    private static final String WORKPATH = "/rule/workpath";
    private static final String PRETASKS = "/rule/pretasks";
    private static final String POSTTASKS = "/rule/posttasks";
    private static final String ERRORTASKS = "/rule/errortasks";
    private static final String TASK = "tasks/task";
    /**
     * Extension of rule files
     */
    private static final String EXT_RULE = ".rule.xml";
    /**
     *
     * @author Frederic Bregier
     *
     */
    static class ExtensionFilter implements FilenameFilter {
        /* (non-Javadoc)
         * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
         */
        @Override
        public boolean accept(File arg0, String arg1) {
            return (arg1.endsWith(EXT_RULE));
        }

    }
    /**
     * Import all Rule files into the HashTable of Rules
     * @param configDirectory
     * @throws OpenR66ProtocolSystemException
     */
    public static void importRules(File configDirectory) throws OpenR66ProtocolSystemException {
        File [] files = FileUtils.getFiles(configDirectory, new ExtensionFilter());
        for (int i = 0; i < files.length; i++) {
            R66Rule rule = getFromFile(files[i]);
            rule.insert();
            logger.warn(rule.toString());
        }
    }
    /**
     *
     * @param document
     * @param path
     * @return The value associated with the path
     * @throws OpenR66ProtocolNoDataException
     */
    private static String getValue(Document document, String path) throws OpenR66ProtocolNoDataException {
        Node node = document.selectSingleNode(path);
        if (node == null) {
            logger.error("Unable to find in Rule file: "+path);
            throw new OpenR66ProtocolNoDataException("Unable to find in the XML Rule file: "+path);
        }
        String result = node.getText();
        if (result == null || result.length() == 0) {
            throw new OpenR66ProtocolNoDataException("Unable to find in the XML Rule file: "+path);
        }
        return result;
    }
    /**
     *
     * @param rootnode
     * @param path
     * @return Null if no tasks or the array of tasks
     */
    @SuppressWarnings("unchecked")
    private static String [][] getTasks(Node rootnode, String path) {
        List<Node> listNode = (List<Node>) rootnode.selectNodes(path);
        if (listNode == null) {
                logger.info("Unable to find the tasks for Rule, setting to the default");
                return null;
        }
        String [][]taskArray = new String[listNode.size()][2];
        for (int i = 0; i < listNode.size(); i++) {
                taskArray[i][0] = null;
                taskArray[i][1] = null;
        }
        for (Node noderoot : listNode) {
                Node nodetype = null, nodepath = null, noderank = null;
                noderank = noderoot.selectSingleNode(R66Rule.TASK_RANK);
                if (noderank == null) {
                        continue;
                }
                int rank = Integer.parseInt(noderank.getText());
                nodetype = noderoot.selectSingleNode(R66Rule.TASK_TYPE);
                if (nodetype == null) {
                        continue;
                }
                nodepath = noderoot.selectSingleNode(R66Rule.TASK_PATH);
                if (nodepath == null) {
                        continue;
                }
                taskArray[rank][0] = nodetype.getText();
                taskArray[rank][1] = nodepath.getText();
        }
        listNode.clear();
        listNode = null;
        return taskArray;
    }
    /**
     *
     * @param file
     * @return the newly created R66Rule from XML File
     * @throws OpenR66ProtocolSystemException
     * @throws OpenR66ProtocolNoDataException
     */
    @SuppressWarnings("unchecked")
    private static R66Rule getFromFile(File file) throws OpenR66ProtocolSystemException {
        R66Rule newRule = null;
        Document document = null;
        // Open config file
        try {
            document = new SAXReader().read(file);
        } catch (DocumentException e) {
            logger.error("Unable to read the XML Rule file: " + file.getName(), e);
            throw new OpenR66ProtocolSystemException("Unable to read the XML Rule file");
        }
        if (document == null) {
            logger.error("Unable to read the XML Rule file: " + file.getName());
            throw new OpenR66ProtocolSystemException("Unable to read the XML Rule file");
        }
        Node nodebase = null;
        String idrule;
        try {
            idrule = getValue(document, IDRULE);
        } catch (OpenR66ProtocolNoDataException e1) {
            throw new OpenR66ProtocolSystemException(e1);
        }
        String recvpath;
        try {
            recvpath = R66Dir.SEPARATOR+getValue(document, RECVPATH);
        } catch (OpenR66ProtocolNoDataException e) {
            recvpath = Configuration.configuration.inPath;
        }
        String sendpath;
        try {
            sendpath = R66Dir.SEPARATOR+getValue(document, SENDPATH);
        } catch (OpenR66ProtocolNoDataException e) {
            sendpath = Configuration.configuration.outPath;
        }
        String archivepath;
        try {
            archivepath = R66Dir.SEPARATOR+getValue(document, ARCHIVEPATH);
        } catch (OpenR66ProtocolNoDataException e) {
            archivepath = Configuration.configuration.archivePath;
        }
        String workpath;
        try {
            workpath = R66Dir.SEPARATOR+getValue(document, WORKPATH);
        } catch (OpenR66ProtocolNoDataException e) {
            workpath = Configuration.configuration.workingPath;
        }

        String []idsArray = null;
        List<Node> listNode = (List<Node>) document.selectNodes(HOSTIDS_HOSTID);
        if (listNode == null) {
            logger.info("Unable to find the id for Rule, setting to the default");
        } else {
            idsArray = new String[listNode.size()];
            int i = 0;
            for (Node nodeid : listNode) {
                    idsArray[i] = nodeid.getText();
                    i++;
            }
            listNode.clear();
            listNode = null;
        }

        nodebase = document.selectSingleNode(PRETASKS);
        String [][]pretasks = getTasks(nodebase, TASK);
        nodebase = document.selectSingleNode(POSTTASKS);
        String [][]posttasks = getTasks(nodebase, TASK);
        nodebase = document.selectSingleNode(ERRORTASKS);
        String [][]errortasks = getTasks(nodebase, TASK);

        newRule = new R66Rule(idrule,idsArray,
                recvpath,sendpath,archivepath,workpath,
                pretasks,posttasks,errortasks);
        return newRule;
    }
}
