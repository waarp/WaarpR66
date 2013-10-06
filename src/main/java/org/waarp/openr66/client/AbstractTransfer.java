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
package org.waarp.openr66.client;

import java.io.File;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66DatabaseGlobalException;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket;
import org.waarp.openr66.protocol.utils.FileUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Abstract class for Transfer operation
 * 
 * @author Frederic Bregier
 * 
 */
public abstract class AbstractTransfer implements Runnable {
	/**
	 * Internal Logger
	 */
	static protected volatile WaarpInternalLogger logger;
	
	protected static String _INFO_ARGS = 
			Messages.getString("AbstractTransfer.0"); //$NON-NLS-1$
	
	protected static final String NO_INFO_ARGS = "noinfo";

	protected final R66Future future;

	protected final String filename;

	protected final String rulename;

	protected final String fileinfo;

	protected final boolean isMD5;

	protected final String remoteHost;

	protected final int blocksize;

	protected final long id;

	protected final Timestamp startTime;

	/**
	 * @param clasz
	 *            Class of Client Transfer
	 * @param future
	 * @param filename
	 * @param rulename
	 * @param fileinfo
	 * @param isMD5
	 * @param remoteHost
	 * @param blocksize
	 * @param id
	 */
	public AbstractTransfer(Class<?> clasz, R66Future future, String filename,
			String rulename, String fileinfo,
			boolean isMD5, String remoteHost, int blocksize, long id, Timestamp timestart) {
		if (logger == null) {
			logger = WaarpInternalLoggerFactory.getLogger(clasz);
		}
		this.future = future;
		this.filename = filename;
		this.rulename = rulename;
		this.fileinfo = fileinfo;
		this.isMD5 = isMD5;
		if (Configuration.configuration.aliases.containsKey(remoteHost)) {
			this.remoteHost = Configuration.configuration.aliases.get(remoteHost);
		} else {
			this.remoteHost = remoteHost;
		}
		this.blocksize = blocksize;
		this.id = id;
		this.startTime = timestart;
	}

	/**
	 * Initiate the Request and return a potential DbTaskRunner
	 * 
	 * @return null if an error occurs or a DbTaskRunner
	 */
	protected DbTaskRunner initRequest() {
		DbRule rule;
		try {
			rule = new DbRule(DbConstant.admin.session, rulename);
		} catch (WaarpDatabaseException e) {
			logger.error("Cannot get Rule: " + rulename, e);
			future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
					ErrorCode.Internal, null));
			future.setFailure(e);
			return null;
		}
		int mode = rule.mode;
		if (isMD5) {
			mode = RequestPacket.getModeMD5(mode);
		}
		DbTaskRunner taskRunner = null;
		if (id != DbConstant.ILLEGALVALUE) {
			try {
				taskRunner = new DbTaskRunner(DbConstant.admin.session, id,
						remoteHost);
			} catch (WaarpDatabaseException e) {
				logger.error("Cannot get task", e);
				future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
						ErrorCode.QueryRemotelyUnknown, null));
				future.setFailure(e);
				return null;
			}
			// requested
			taskRunner.setSenderByRequestToValidate(true);
			if (fileinfo != null && !fileinfo.equals(NO_INFO_ARGS)) {
				taskRunner.setFileInformation(fileinfo);
			}
			if (startTime != null) {
				taskRunner.setStart(startTime);
			}
		} else {
			long originalSize = -1;
			if (RequestPacket.isSendMode(mode) && ! RequestPacket.isThroughMode(mode)) {
				File file = new File(filename);
				// Change dir
				try {
					R66Session session = new R66Session();
					session.getAuth().specialNoSessionAuth(false, Configuration.configuration.HOST_ID);
					session.getDir().changeDirectory(rule.getSendPath());
					R66File filer66 = FileUtils.getFile(logger, session, filename, true, true, false, null);
					file = filer66.getTrueFile();
				} catch (CommandAbstractException e) {
				} catch (OpenR66RunnerErrorException e) {
				}
				if (file.canRead()) {
					originalSize = file.length();
				}
			}
			logger.debug("Filesize: "+originalSize);
			String sep = PartnerConfiguration.getSeparator(remoteHost);
			RequestPacket request = new RequestPacket(rulename,
					mode, filename, blocksize, 0,
					id, fileinfo, originalSize, sep);
			// Not isRecv since it is the requester, so send => isRetrieve is true
			boolean isRetrieve = !RequestPacket.isRecvMode(request.getMode());
			try {
				taskRunner =
						new DbTaskRunner(DbConstant.admin.session, rule, isRetrieve, request,
								remoteHost, startTime);
			} catch (WaarpDatabaseException e) {
				logger.error("Cannot get task", e);
				future.setResult(new R66Result(new OpenR66DatabaseGlobalException(e), null, true,
						ErrorCode.Internal, null));
				future.setFailure(e);
				return null;
			}
		}
		return taskRunner;
	}

	static protected String rhost = null;
	static protected String localFilename = null;
	static protected String rule = null;
	static protected String fileInfo = null;
	static protected boolean ismd5 = false;
	static protected int block = 0x10000; // 64K
											// as
											// default
	static protected boolean nolog = false;
	static protected long idt = DbConstant.ILLEGALVALUE;
	static protected Timestamp ttimestart = null;
	static protected SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	/**
	 * Parse the parameter and set current values
	 * 
	 * @param args
	 * @param submitOnly
	 *            True if the client is only a submitter (through database)
	 * @return True if all parameters were found and correct
	 */
	protected static boolean getParams(String[] args, boolean submitOnly) {
		_INFO_ARGS = Messages.getString("AbstractTransfer.0"); //$NON-NLS-1$
		if (args.length < 2) {
			logger
					.error(_INFO_ARGS);
			return false;
		}
		if (submitOnly) {
			if (!FileBasedConfiguration
					.setSubmitClientConfigurationFromXml(Configuration.configuration, args[0])) {
				logger
						.error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
				return false;
			}
		} else if (!FileBasedConfiguration
				.setClientConfigurationFromXml(Configuration.configuration, args[0])) {
			logger
					.error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
			return false;
		}
		// Now set default values from configuration
		block = Configuration.configuration.BLOCKSIZE;
		int i = 1;
		try {
			for (i = 1; i < args.length; i++) {
				if (args[i].equalsIgnoreCase("-to")) {
					i++;
					rhost = args[i];
				} else if (args[i].equalsIgnoreCase("-file")) {
					i++;
					localFilename = args[i];
				} else if (args[i].equalsIgnoreCase("-rule")) {
					i++;
					rule = args[i];
				} else if (args[i].equalsIgnoreCase("-info")) {
					i++;
					fileInfo = args[i];
				} else if (args[i].equalsIgnoreCase("-md5")) {
					ismd5 = true;
				} else if (args[i].equalsIgnoreCase("-block")) {
					i++;
					block = Integer.parseInt(args[i]);
					if (block < 100) {
						logger.error(Messages.getString("AbstractTransfer.1") + block); //$NON-NLS-1$
						return false;
					}
				} else if (args[i].equalsIgnoreCase("-nolog")) {
					nolog = true;
				} else if (args[i].equalsIgnoreCase("-id")) {
					i++;
					idt = Long.parseLong(args[i]);
				} else if (args[i].equalsIgnoreCase("-start")) {
					i++;
					Date date;
					try {
						date = dateFormat.parse(args[i]);
						ttimestart = new Timestamp(date.getTime());
					} catch (ParseException e) {
					}
				} else if (args[i].equalsIgnoreCase("-delay")) {
					i++;
					if (args[i].charAt(0) == '+') {
						ttimestart = new Timestamp(System.currentTimeMillis() +
								Long.parseLong(args[i].substring(1)));
					} else {
						ttimestart = new Timestamp(Long.parseLong(args[i]));
					}
				}
			}
		} catch (NumberFormatException e) {
			logger.error(Messages.getString("AbstractTransfer.20")+i); //$NON-NLS-1$
			return false;
		}
		if (fileInfo == null) {
			fileInfo = NO_INFO_ARGS;
		}
		if (rhost != null && rule != null && localFilename != null) {
			return true;
		} else if (idt != DbConstant.ILLEGALVALUE && rhost != null) {
			try {
				DbTaskRunner runner = new DbTaskRunner(DbConstant.admin.session, idt,
						rhost);
				rule = runner.getRuleId();
				localFilename = runner.getOriginalFilename();
				return true;
			} catch (WaarpDatabaseException e) {
				logger.error(
						Messages.getString("AbstractTransfer.21") //$NON-NLS-1$
								, e);
				return false;
			}

		}
		logger.error(Messages.getString("AbstractTransfer.21") + //$NON-NLS-1$
				_INFO_ARGS);
		return false;
	}
}
