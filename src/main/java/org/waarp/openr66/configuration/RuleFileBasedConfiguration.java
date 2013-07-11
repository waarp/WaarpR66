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
package org.waarp.openr66.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.DefaultElement;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.file.DirInterface;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.xml.XmlDecl;
import org.waarp.common.xml.XmlHash;
import org.waarp.common.xml.XmlType;
import org.waarp.common.xml.XmlUtil;
import org.waarp.common.xml.XmlValue;
import org.waarp.openr66.context.task.TaskType;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.utils.FileUtils;

/**
 * Rule File Based Configuration
 * 
 * @author Frederic Bregier
 * 
 */
public class RuleFileBasedConfiguration {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(RuleFileBasedConfiguration.class);

	private static final String MULTIPLEROOT = "rules";
	private static final String ROOT = "rule";
	private static final String XIDRULE = "idrule";
	public static final String XHOSTIDS = "hostids";
	public static final String XHOSTID = "hostid";
	private static final String XMODE = "mode";
	private static final String XRECVPATH = "recvpath";
	private static final String XSENDPATH = "sendpath";
	private static final String XARCHIVEPATH = "archivepath";
	private static final String XWORKPATH = "workpath";
	private static final String XRPRETASKS = "rpretasks";
	private static final String XRPOSTTASKS = "rposttasks";
	private static final String XRERRORTASKS = "rerrortasks";
	private static final String XSPRETASKS = "spretasks";
	private static final String XSPOSTTASKS = "sposttasks";
	private static final String XSERRORTASKS = "serrortasks";
	public static final String XTASKS = "tasks";
	public static final String XTASK = "task";

	private static final String HOSTIDS_HOSTID = "/" + XHOSTIDS + "/"
			+ XHOSTID;

	private static final String TASK = "/tasks/task";

	private static final XmlDecl[] taskDecl = {
			new XmlDecl(XmlType.STRING, DbRule.TASK_TYPE),
			new XmlDecl(XmlType.STRING, DbRule.TASK_PATH),
			new XmlDecl(XmlType.LONG, DbRule.TASK_DELAY),
	};
	public static final XmlDecl[] tasksDecl = {
			new XmlDecl(XTASK,
					XmlType.XVAL, TASK,
					taskDecl, true)
	};
	private static final XmlDecl[] subruleDecls = {
			new XmlDecl(XmlType.STRING, XIDRULE),
			new XmlDecl(XHOSTIDS, XmlType.STRING, HOSTIDS_HOSTID, true),
			new XmlDecl(XmlType.INTEGER, XMODE),
			new XmlDecl(XmlType.STRING, XRECVPATH),
			new XmlDecl(XmlType.STRING, XSENDPATH),
			new XmlDecl(XmlType.STRING, XARCHIVEPATH),
			new XmlDecl(XmlType.STRING, XWORKPATH),
			new XmlDecl(XRPRETASKS, XmlType.XVAL, XRPRETASKS, tasksDecl, false),
			new XmlDecl(XRPOSTTASKS, XmlType.XVAL, XRPOSTTASKS, tasksDecl, false),
			new XmlDecl(XRERRORTASKS, XmlType.XVAL, XRERRORTASKS, tasksDecl, false),
			new XmlDecl(XSPRETASKS, XmlType.XVAL, XSPRETASKS, tasksDecl, false),
			new XmlDecl(XSPOSTTASKS, XmlType.XVAL, XSPOSTTASKS, tasksDecl, false),
			new XmlDecl(XSERRORTASKS, XmlType.XVAL, XSERRORTASKS, tasksDecl, false)
	};
	private static final XmlDecl[] ruleDecls = {
			new XmlDecl(ROOT, XmlType.XVAL,
					ROOT, subruleDecls,
					false)
	};
	private static final XmlDecl[] multipleruleDecls = {
			new XmlDecl(MULTIPLEROOT,
					XmlType.XVAL, "/"
							+ MULTIPLEROOT
							+ "/" + ROOT,
					subruleDecls, true)
	};
	public static final XmlDecl[] hostsDecls = {
			new XmlDecl(XHOSTIDS,
					XmlType.STRING,
					HOSTIDS_HOSTID, true),
	};

	/**
	 * Extension of rule files
	 */
	public static final String EXT_RULE = ".rule.xml";
	/**
	 * Extension of multiple rules in one file
	 */
	public static final String EXT_RULES = ".rules.xml";

	/**
	 * Import all Rule files into the HashTable of Rules
	 * 
	 * @param configDirectory
	 * @throws OpenR66ProtocolSystemException
	 * @throws WaarpDatabaseException
	 */
	public static void importRules(File configDirectory)
			throws OpenR66ProtocolSystemException, WaarpDatabaseException {
		File[] files = FileUtils.getFiles(configDirectory,
				new ExtensionFilter(EXT_RULE));
		for (File file : files) {
			DbRule rule = getFromFile(file);
			logger.debug(rule.toString());
		}
		files = FileUtils.getFiles(configDirectory,
				new ExtensionFilter(EXT_RULES));
		for (File file : files) {
			getMultipleFromFile(file);
		}
	}

	/**
	 * Utility function
	 * 
	 * @param value
	 * @return the array of tasks or empty array if in error.
	 */
	@SuppressWarnings("unchecked")
	public static String[][] getTasksRule(XmlValue value) {
		List<XmlValue[]> list = (List<XmlValue[]>) value.getList();
		if (list == null || list.isEmpty()) {
			logger.debug("NoRule for " + value.getName());
			// Unable to find the tasks for Rule, setting to the default
			return new String[0][0];
		}
		String[][] taskArray = new String[list.size()][3];
		for (int i = 0; i < list.size(); i++) {
			taskArray[i][0] = null;
			taskArray[i][1] = null;
			taskArray[i][2] = null;
		}
		int rank = 0;
		for (XmlValue[] subvals : list) {
			XmlHash hash = new XmlHash(subvals);
			XmlValue valtype = hash.get(DbRule.TASK_TYPE);
			if (valtype == null || (valtype.isEmpty()) || valtype.getString().isEmpty()) {
				continue;
			}
			XmlValue valpath = hash.get(DbRule.TASK_PATH);
			if (valpath == null || (valpath.isEmpty()) || valtype.getString().isEmpty()) {
				continue;
			}
			XmlValue valdelay = hash.get(DbRule.TASK_DELAY);
			String delay;
			if (valdelay == null || (valdelay.isEmpty())) {
				delay = Long
						.toString(Configuration.configuration.TIMEOUTCON);
			} else {
				delay = valdelay.getIntoString();
			}
			taskArray[rank][0] = valtype.getString().toUpperCase();
			// CHECK TASK_TYPE
			try {
				TaskType.valueOf(taskArray[rank][0]);
			} catch (IllegalArgumentException e) {
				// Bad Type
				logger.warn("Bad Type of Task: " + taskArray[rank][0]);
				continue;
			}
			taskArray[rank][1] = valpath.getString();
			taskArray[rank][2] = delay;
			rank++;
			hash.clear();
		}
		list.clear();
		list = null;
		return taskArray;
	}

	/**
	 * 
	 * @param value
	 *            the XmlValue hosting hostids/hostid
	 * @return the array of HostIds allowed for the current rule
	 */
	public static String[] getHostIds(XmlValue value) {
		String[] idsArray = null;
		if (value == null || (value.getList() == null) || value.getList().isEmpty()) {
			logger
					.info("Unable to find the id for Rule, setting to the default");
		} else {
			@SuppressWarnings("unchecked")
			List<String> ids = (List<String>) value.getList();
			idsArray = new String[ids.size()];
			int i = 0;
			for (String sval : ids) {
				if (sval.isEmpty()) {
					continue;
				}
				idsArray[i] = sval;
				i++;
			}
			ids.clear();
			ids = null;
		}
		return idsArray;
	}

	/**
	 * Load and update a Rule from a file
	 * 
	 * @param file
	 * @return the newly created R66Rule from XML File
	 * @throws OpenR66ProtocolSystemException
	 * @throws WaarpDatabaseException
	 * @throws WaarpDatabaseNoDataException
	 * @throws WaarpDatabaseSqlException
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws OpenR66ProtocolNoDataException
	 */
	public static DbRule getFromFile(File file)
			throws OpenR66ProtocolSystemException, WaarpDatabaseNoConnectionException,
			WaarpDatabaseSqlException, WaarpDatabaseNoDataException, WaarpDatabaseException {
		DbRule newRule = null;
		Document document = null;
		// Open config file
		try {
			document = new SAXReader().read(file);
		} catch (DocumentException e) {
			logger.error("Unable to read the XML Rule file: " + file.getName(),
					e);
			throw new OpenR66ProtocolSystemException(
					"Unable to read the XML Rule file", e);
		}
		if (document == null) {
			logger.error("Unable to read the XML Rule file: " + file.getName());
			throw new OpenR66ProtocolSystemException(
					"Unable to read the XML Rule file");
		}
		XmlValue[] values = XmlUtil.read(document, ruleDecls);
		newRule = getFromXmlValue(values);
		values = null;
		return newRule;
	}

	/**
	 * Load and update multiple Rules from one file
	 * 
	 * @param file
	 * @return a list of newly created R66Rule from XML File
	 * @throws OpenR66ProtocolSystemException
	 * @throws WaarpDatabaseException
	 * @throws WaarpDatabaseNoDataException
	 * @throws WaarpDatabaseSqlException
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws OpenR66ProtocolNoDataException
	 */
	public static List<DbRule> getMultipleFromFile(File file)
			throws OpenR66ProtocolSystemException, WaarpDatabaseNoConnectionException,
			WaarpDatabaseSqlException, WaarpDatabaseNoDataException, WaarpDatabaseException {
		Document document = null;
		// Open config file
		try {
			document = new SAXReader().read(file);
		} catch (DocumentException e) {
			logger.error("Unable to read the XML Rule file: " + file.getName(),
					e);
			throw new OpenR66ProtocolSystemException(
					"Unable to read the XML Rule file", e);
		}
		if (document == null) {
			logger.error("Unable to read the XML Rule file: " + file.getName());
			throw new OpenR66ProtocolSystemException(
					"Unable to read the XML Rule file");
		}
		XmlValue[] values = XmlUtil.read(document, multipleruleDecls);
		if (values.length <= 0) {
			return new ArrayList<DbRule>(0);
		}
		XmlValue value = values[0];
		@SuppressWarnings("unchecked")
		List<XmlValue[]> list = (List<XmlValue[]>) value.getList();
		List<DbRule> result = new ArrayList<DbRule>(list.size());
		for (XmlValue[] xmlValue : list) {
			result.add(getFromXmlValue(xmlValue));
		}
		values = null;
		return result;
	}

	/**
	 * Load and update one Rule from a XmlValue
	 * 
	 * @param root
	 * @return the newly created R66Rule from XML File
	 * @throws OpenR66ProtocolSystemException
	 * @throws WaarpDatabaseException
	 * @throws WaarpDatabaseNoDataException
	 * @throws WaarpDatabaseSqlException
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws OpenR66ProtocolNoDataException
	 */
	private static DbRule getFromXmlValue(XmlValue[] root)
			throws OpenR66ProtocolSystemException, WaarpDatabaseNoConnectionException,
			WaarpDatabaseSqlException, WaarpDatabaseNoDataException, WaarpDatabaseException {
		DbRule newRule = null;
		XmlHash hash = new XmlHash(root);
		XmlValue value = hash.get(XIDRULE);
		if (value == null || (value.isEmpty()) || value.getString().length() == 0) {
			logger.error("Unable to find in Rule field: " + XIDRULE);
			throw new OpenR66ProtocolSystemException();
		}
		String idrule = value.getString();
		value = hash.get(XMODE);
		if (value == null || (value.isEmpty())) {
			logger.error("Unable to find in Rule field: " + XMODE);
			throw new OpenR66ProtocolSystemException();
		}
		int mode = value.getInteger();
		String recvpath;
		value = hash.get(XRECVPATH);
		if (value == null || (value.isEmpty()) || value.getString().length() == 0) {
			recvpath = Configuration.configuration.inPath;
		} else {
			recvpath = DirInterface.SEPARATOR + value.getString();
		}
		String sendpath;
		value = hash.get(XSENDPATH);
		if (value == null || (value.isEmpty()) || value.getString().length() == 0) {
			sendpath = Configuration.configuration.outPath;
		} else {
			sendpath = DirInterface.SEPARATOR + value.getString();
		}
		String archivepath;
		value = hash.get(XARCHIVEPATH);
		if (value == null || (value.isEmpty()) || value.getString().length() == 0) {
			archivepath = Configuration.configuration.archivePath;
		} else {
			archivepath = DirInterface.SEPARATOR + value.getString();
		}
		String workpath;
		value = hash.get(XWORKPATH);
		if (value == null || (value.isEmpty()) || value.getString().length() == 0) {
			workpath = Configuration.configuration.workingPath;
		} else {
			workpath = DirInterface.SEPARATOR + value.getString();
		}
		String[] idsArray = null;
		value = hash.get(XHOSTIDS);
		idsArray = getHostIds(value);
		String[][] rpretasks = new String[0][0];
		value = hash.get(XRPRETASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				rpretasks = getTasksRule(subvalues[0]);
			}
		}
		String[][] rposttasks = new String[0][0];
		value = hash.get(XRPOSTTASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				rposttasks = getTasksRule(subvalues[0]);
			}
		}
		String[][] rerrortasks = new String[0][0];
		value = hash.get(XRERRORTASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				rerrortasks = getTasksRule(subvalues[0]);
			}
		}
		String[][] spretasks = new String[0][0];
		value = hash.get(XSPRETASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				spretasks = getTasksRule(subvalues[0]);
			}
		}
		String[][] sposttasks = new String[0][0];
		value = hash.get(XSPOSTTASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				sposttasks = getTasksRule(subvalues[0]);
			}
		}
		String[][] serrortasks = new String[0][0];
		value = hash.get(XSERRORTASKS);
		if (value != null && (!value.isEmpty())) {
			XmlValue[] subvalues = value.getSubXml();
			if (subvalues.length > 0) {
				serrortasks = getTasksRule(subvalues[0]);
			}
		}

		newRule = new DbRule(DbConstant.admin.session, idrule, idsArray, mode, recvpath, sendpath,
				archivepath, workpath, rpretasks, rposttasks, rerrortasks,
				spretasks, sposttasks, serrortasks);
		if (DbConstant.admin != null && DbConstant.admin.session != null) {
			if (newRule.exist()) {
				newRule.update();
			} else {
				newRule.insert();
			}
		} else {
			// put in hashtable
			newRule.insert();
		}
		hash.clear();
		return newRule;
	}

	/**
	 * Construct a new Element with value
	 * 
	 * @param name
	 * @param value
	 * @return the new Element
	 */
	private static final Element newElement(String name, String value) {
		Element node = new DefaultElement(name);
		node.addText(value);
		return node;
	}

	/**
	 * Add a rule from root element (ROOT = rule)
	 * 
	 * @param element
	 * @param rule
	 */
	private static void addToElement(Element element, DbRule rule) {
		Element root = element;
		root.add(newElement(XIDRULE, rule.idRule));
		Element hosts = new DefaultElement(XHOSTIDS);
		if (rule.idsArray != null) {
			for (String host : rule.idsArray) {
				hosts.add(newElement(XHOSTID, host));
			}
		}
		root.add(hosts);
		root.add(newElement(XMODE, Integer.toString(rule.mode)));
		String dir = rule.getRuleRecvPath();
		if (dir != null) {
			if (dir.startsWith(File.separator)) {
				root.add(newElement(XRECVPATH, dir.substring(1)));
			} else {
				root.add(newElement(XRECVPATH, dir));
			}
		}
		dir = rule.getRuleSendPath();
		if (dir != null) {
			if (dir.startsWith(File.separator)) {
				root.add(newElement(XSENDPATH, dir.substring(1)));
			} else {
				root.add(newElement(XSENDPATH, dir));
			}
		}
		dir = rule.getRuleArchivePath();
		if (dir != null) {
			if (dir.startsWith(File.separator)) {
				root.add(newElement(XARCHIVEPATH, dir.substring(1)));
			} else {
				root.add(newElement(XARCHIVEPATH, dir));
			}
		}
		dir = rule.getRuleWorkPath();
		if (dir != null) {
			if (dir.startsWith(File.separator)) {
				root.add(newElement(XWORKPATH, dir.substring(1)));
			} else {
				root.add(newElement(XWORKPATH, dir));
			}
		}
		Element tasks = new DefaultElement(XRPRETASKS);
		Element roottasks = new DefaultElement(XTASKS);
		int rank = 0;
		String[][] array = rule.rpreTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
		tasks = new DefaultElement(XRPOSTTASKS);
		roottasks = new DefaultElement(XTASKS);
		array = rule.rpostTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
		tasks = new DefaultElement(XRERRORTASKS);
		roottasks = new DefaultElement(XTASKS);
		array = rule.rerrorTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
		tasks = new DefaultElement(XSPRETASKS);
		roottasks = new DefaultElement(XTASKS);
		array = rule.spreTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
		tasks = new DefaultElement(XSPOSTTASKS);
		roottasks = new DefaultElement(XTASKS);
		array = rule.spostTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
		tasks = new DefaultElement(XSERRORTASKS);
		roottasks = new DefaultElement(XTASKS);
		array = rule.serrorTasksArray;
		if (array != null) {
			for (rank = 0; rank < array.length; rank++) {
				Element task = new DefaultElement(XTASK);
				task.add(newElement(DbRule.TASK_TYPE, array[rank][0]));
				task.add(newElement(DbRule.TASK_PATH, array[rank][1]));
				task.add(newElement(DbRule.TASK_DELAY, array[rank][2]));
				roottasks.add(task);
			}
		}
		tasks.add(roottasks);
		root.add(tasks);
	}

	/**
	 * Write the rule to a file from filename
	 * 
	 * @param filename
	 * @param rule
	 * @throws OpenR66ProtocolSystemException
	 */
	private static final void writeXML(String filename, DbRule rule)
			throws OpenR66ProtocolSystemException {
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement(ROOT);
		addToElement(root, rule);
		try {
			XmlUtil.writeXML(filename, null, document);
		} catch (IOException e) {
			throw new OpenR66ProtocolSystemException("Cannot write file: " + filename, e);
		}
	}

	/**
	 * Write to directory files prefixed by hostname all Rules from database
	 * 
	 * @param directory
	 * @param hostname
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws WaarpDatabaseSqlException
	 * @throws OpenR66ProtocolSystemException
	 */
	public static final void writeXml(String directory, String hostname)
			throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException,
			OpenR66ProtocolSystemException {
		File dir = new File(directory);
		if (!dir.isDirectory()) {
			dir.mkdirs();
		}
		DbRule[] rules = DbRule.getAllRules(DbConstant.admin.session);
		for (DbRule rule : rules) {
			String filename = dir.getAbsolutePath() + File.separator + hostname + "_" + rule.idRule
					+
					RuleFileBasedConfiguration.EXT_RULE;
			logger.debug("Will write Rule: "+rule.idRule+" in "+filename);
			RuleFileBasedConfiguration.writeXML(filename, rule);
		}
	}

	/**
	 * Write to directory 1 file prefixed by hostname all Rules from database
	 * 
	 * @param directory
	 * @param hostname
	 * @return the filename
	 * @throws WaarpDatabaseNoConnectionException
	 * @throws WaarpDatabaseSqlException
	 * @throws OpenR66ProtocolSystemException
	 */
	public static String writeOneXml(String directory, String hostname)
			throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException,
			OpenR66ProtocolSystemException {
		File dir = new File(directory);
		if (!dir.isDirectory()) {
			dir.mkdirs();
		}
		DbRule[] rules = DbRule.getAllRules(DbConstant.admin.session);
		String filename = dir.getAbsolutePath() + File.separator + hostname +
				RuleFileBasedConfiguration.EXT_RULES;
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement(MULTIPLEROOT);
		for (DbRule rule : rules) {
			Element element = root.addElement(ROOT);
			addToElement(element, rule);
		}
		try {
			XmlUtil.writeXML(filename, null, document);
		} catch (IOException e) {
			throw new OpenR66ProtocolSystemException("Cannot write file: " + filename, e);
		}
		return filename;
	}
}
