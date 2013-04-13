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
package org.waarp.openr66.context.task;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66Dir;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Abstract implementation of task
 * 
 * @author Frederic Bregier
 * 
 */
public abstract class AbstractTask implements Runnable {
	/**
	 * Current full path of current FILENAME
	 */
	public static final String TRUEFULLPATH = "#TRUEFULLPATH#";

	/**
	 * Current FILENAME (basename) (change in retrieval part)
	 */
	public static final String TRUEFILENAME = "#TRUEFILENAME#";
	/**
	 * Current full path of Original FILENAME (as transmitted) (before changing in retrieval part)
	 */
	public static final String ORIGINALFULLPATH = "#ORIGINALFULLPATH#";

	/**
	 * Original FILENAME (basename) (before changing in retrieval part)
	 */
	public static final String ORIGINALFILENAME = "#ORIGINALFILENAME#";

	/**
	 * Size of the current FILE
	 */
	public static final String FILESIZE = "#FILESIZE#";

	/**
	 * Current full path of current RULE
	 */
	public static final String RULE = "#RULE#";

	/**
	 * Date in yyyyMMdd format
	 */
	public static final String DATE = "#DATE#";

	/**
	 * Hour in HHmmss format
	 */
	public static final String HOUR = "#HOUR#";

	/**
	 * Remote host id (if not the initiator of the call)
	 */
	public static final String REMOTEHOST = "#REMOTEHOST#";

	/**
	 * Remote host address
	 */
	public static final String REMOTEHOSTADDR = "#REMOTEHOSTADDR#";

	/**
	 * Local host id
	 */
	public static final String LOCALHOST = "#LOCALHOST#";

	/**
	 * Local host address
	 */
	public static final String LOCALHOSTADDR = "#LOCALHOSTADDR#";

	/**
	 * Transfer id
	 */
	public static final String TRANSFERID = "#TRANSFERID#";

	/**
	 * Requester Host
	 */
	public static final String REQUESTERHOST = "#REQUESTERHOST#";

	/**
	 * Requested Host
	 */
	public static final String REQUESTEDHOST = "#REQUESTEDHOST#";

	/**
	 * Full Transfer id (TRANSFERID_REQUESTERHOST_REQUESTEDHOST)
	 */
	public static final String FULLTRANSFERID = "#FULLTRANSFERID#";

	/**
	 * Current or final RANK of block
	 */
	public static final String RANKTRANSFER = "#RANKTRANSFER#";

	/**
	 * Block size used
	 */
	public static final String BLOCKSIZE = "#BLOCKSIZE#";

	/**
	 * IN Path used
	 */
	public static final String INPATH = "#INPATH#";

	/**
	 * OUT Path used
	 */
	public static final String OUTPATH = "#OUTPATH#";

	/**
	 * WORK Path used
	 */
	public static final String WORKPATH = "#WORKPATH#";

	/**
	 * ARCH Path used
	 */
	public static final String ARCHPATH = "#ARCHPATH#";

	/**
	 * HOME Path used
	 */
	public static final String HOMEPATH = "#HOMEPATH#";
	/**
	 * Last Current Error Message
	 */
	public static final String ERRORMSG = "#ERRORMSG#";
	/**
	 * Last Current Error Code
	 */
	public static final String ERRORCODE = "#ERRORCODE#";
	/**
	 * Last Current Error Code in Full String
	 */
	public static final String ERRORSTRCODE = "#ERRORSTRCODE#";
	/**
	 * If specified, no Wait for Task Validation (default is wait)
	 */
	public static final String NOWAIT = "#NOWAIT#";
	/**
	 * If specified, use the LocalExec Daemon specified in the global configuration (default no
	 * usage of LocalExec)
	 */
	public static final String LOCALEXEC = "#LOCALEXEC#";
	/**
	 * Delimiter for -count option in Reschedule to be placed in the info of transfer as
	 * #CPTLIMIT#limit#CPTLIMIT# where limit is an integer.
	 */
	public static final String CPTLIMIT = "#CPTLIMIT#";

	/**
	 * Type of operation
	 */
	final TaskType type;

	/**
	 * Argument from Rule
	 */
	final String argRule;

	/**
	 * Delay from Rule (if applicable)
	 */
	final int delay;

	/**
	 * Argument from Transfer
	 */
	final String argTransfer;

	/**
	 * Current session
	 */
	final R66Session session;

	/**
	 * R66Future of completion
	 */
	final R66Future futureCompletion;
	/**
	 * Do we wait for a validation of the task ? Default = True
	 */
	boolean waitForValidation = true;
	/**
	 * Do we need to use LocalExec for an Exec Task ? Default = False
	 */
	boolean useLocalExec = false;

	/**
	 * Constructor
	 * 
	 * @param type
	 * @param delay
	 * @param arg
	 * @param session
	 */
	AbstractTask(TaskType type, int delay, String argRule, String argTransfer,
			R66Session session) {
		this.type = type;
		this.delay = delay;
		this.argRule = argRule;
		this.argTransfer = argTransfer;
		this.session = session;
		futureCompletion = new R66Future(true);
	}

	/**
	 * This is the only interface to execute an operator.
	 */
	abstract public void run();

	/**
	 * 
	 * @return True if the operation is in success status
	 */
	public boolean isSuccess() {
		futureCompletion.awaitUninterruptibly();
		return futureCompletion.isSuccess();
	}

	/**
	 * 
	 * @return the R66Future of completion
	 */
	public R66Future getFutureCompletion() {
		return futureCompletion;
	}

	/**
	 * 
	 * @param arg
	 *            as the Format string where FIXED items will be replaced by context values and next
	 *            using argFormat as format second argument; this arg comes from the rule itself
	 * @param argFormat
	 *            as format second argument; this argFormat comes from the transfer Information
	 *            itself
	 * @return The string with replaced values from context and second argument
	 */
	protected String getReplacedValue(String arg, Object[] argFormat) {
		StringBuilder builder = new StringBuilder(arg);
		// check NOWAIT and LOCALEXEC
		if (arg.contains(NOWAIT)) {
			waitForValidation = false;
			WaarpStringUtils.replaceAll(builder, NOWAIT, "");
		}
		if (arg.contains(LOCALEXEC)) {
			useLocalExec = true;
			WaarpStringUtils.replaceAll(builder, LOCALEXEC, "");
		}
		File trueFile = null;
		if (session.getFile() != null) {
			trueFile = session.getFile().getTrueFile();
		}
		if (trueFile != null) {
			WaarpStringUtils.replaceAll(builder, TRUEFULLPATH, trueFile.getAbsolutePath());
			WaarpStringUtils.replaceAll(builder, TRUEFILENAME, R66Dir
					.getFinalUniqueFilename(session.getFile()));
			WaarpStringUtils.replaceAll(builder, FILESIZE, Long.toString(trueFile.length()));
		} else {
			WaarpStringUtils.replaceAll(builder, TRUEFULLPATH, "nofile");
			WaarpStringUtils.replaceAll(builder, TRUEFILENAME, "nofile");
			WaarpStringUtils.replaceAll(builder, FILESIZE, "0");
		}
		WaarpStringUtils.replaceAll(builder, ORIGINALFULLPATH, session.getRunner()
				.getOriginalFilename());
		WaarpStringUtils.replaceAll(builder, ORIGINALFILENAME,
				R66File.getBasename(session.getRunner()
						.getOriginalFilename()));
		WaarpStringUtils.replaceAll(builder, RULE, session.getRunner()
				.getRuleId());
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date();
		WaarpStringUtils.replaceAll(builder, DATE, dateFormat.format(date));
		dateFormat = new SimpleDateFormat("HHmmss");
		WaarpStringUtils.replaceAll(builder, HOUR, dateFormat.format(date));
		if (session.getAuth() != null) {
			WaarpStringUtils.replaceAll(builder, REMOTEHOST, session.getAuth().getUser());
			try {
				WaarpStringUtils.replaceAll(builder, LOCALHOST,
						Configuration.configuration.getHostId(session.getAuth().isSsl()));
			} catch (OpenR66ProtocolNoSslException e) {
				// replace by standard name
				WaarpStringUtils.replaceAll(builder, LOCALHOST,
						Configuration.configuration.HOST_ID);
			}
		}
		if (session.getRemoteAddress() != null) {
			WaarpStringUtils.replaceAll(builder, REMOTEHOSTADDR, session.getRemoteAddress()
					.toString());
			WaarpStringUtils.replaceAll(builder, LOCALHOSTADDR, session.getLocalAddress()
					.toString());
		} else {
			WaarpStringUtils.replaceAll(builder, REMOTEHOSTADDR, "unknown");
			WaarpStringUtils.replaceAll(builder, LOCALHOSTADDR, "unknown");
		}
		WaarpStringUtils.replaceAll(builder, TRANSFERID, Long.toString(session
				.getRunner().getSpecialId()));
		String requester = session.getRunner().getRequester();
		WaarpStringUtils.replaceAll(builder, REQUESTERHOST, requester);
		String requested = session.getRunner().getRequested();
		WaarpStringUtils.replaceAll(builder, REQUESTEDHOST, requested);
		WaarpStringUtils.replaceAll(builder, FULLTRANSFERID, session
				.getRunner().getSpecialId() + "_" + requester + "_" + requested);
		WaarpStringUtils.replaceAll(builder, RANKTRANSFER, Integer.toString(session
				.getRunner().getRank()));
		WaarpStringUtils.replaceAll(builder, BLOCKSIZE, Integer.toString(session
				.getBlockSize()));
		R66Dir dir = new R66Dir(session);
		if (session.getRunner().isRecvThrough() || session.getRunner().isSendThrough()) {
			try {
				dir.changeDirectoryNotChecked(session.getRunner().getRule().recvPath);
				WaarpStringUtils.replaceAll(builder, INPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectoryNotChecked(session.getRunner().getRule().sendPath);
				WaarpStringUtils.replaceAll(builder, OUTPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectoryNotChecked(session.getRunner().getRule().workPath);
				WaarpStringUtils.replaceAll(builder, WORKPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectoryNotChecked(session.getRunner().getRule().archivePath);
				WaarpStringUtils.replaceAll(builder, ARCHPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
		} else {
			try {
				dir.changeDirectoryNotChecked(session.getRunner().getRule().recvPath);
				WaarpStringUtils.replaceAll(builder, INPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectory(session.getRunner().getRule().sendPath);
				WaarpStringUtils.replaceAll(builder, OUTPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectory(session.getRunner().getRule().workPath);
				WaarpStringUtils.replaceAll(builder, WORKPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
			dir = new R66Dir(session);
			try {
				dir.changeDirectory(session.getRunner().getRule().archivePath);
				WaarpStringUtils.replaceAll(builder, ARCHPATH, dir.getFullPath());
			} catch (CommandAbstractException e) {
			}
		}
		WaarpStringUtils.replaceAll(builder, HOMEPATH, Configuration.configuration.baseDirectory);
		if (session.getLocalChannelReference() == null) {
			WaarpStringUtils.replaceAll(builder, ERRORMSG, "NoError");
			WaarpStringUtils.replaceAll(builder, ERRORCODE, "-");
			WaarpStringUtils.replaceAll(builder, ERRORSTRCODE, ErrorCode.Unknown.name());
		} else {
			try {
				WaarpStringUtils.replaceAll(builder, ERRORMSG, session.getLocalChannelReference()
						.getErrorMessage());
			} catch (NullPointerException e) {
				WaarpStringUtils.replaceAll(builder, ERRORMSG, "NoError");
			}
			try {
				WaarpStringUtils.replaceAll(builder, ERRORCODE, session.getLocalChannelReference()
						.getCurrentCode().getCode());
			} catch (NullPointerException e) {
				WaarpStringUtils.replaceAll(builder, ERRORCODE, "-");
			}
			try {
				WaarpStringUtils.replaceAll(builder, ERRORSTRCODE, session
						.getLocalChannelReference().getCurrentCode().name());
			} catch (NullPointerException e) {
				WaarpStringUtils.replaceAll(builder, ERRORSTRCODE, ErrorCode.Unknown.name());
			}
		}
		// finalname
		if (argFormat != null && argFormat.length > 0)
			return String.format(builder.toString(), argFormat);
		return builder.toString();
	}
}
