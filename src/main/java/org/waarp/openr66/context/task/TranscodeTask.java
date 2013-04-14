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

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.transcode.CharsetsUtil;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * Transcode the current file from one Charset to another Charset as specified<br>
 * <br>
 * Arguments are:<br>
 * -from charset<br>
 * -to charset<br>
 * -newfile newfilename ; optional argument ; if not used, will be current filename.extension ; 
 * if used, extension is ignored<br>
 * -extension extension ; optional argument ; if not used, will be filename.transcode<br>
 * <br>
 * A convenient method (from Waarp Common) allows to list in html (-html), csv (-csv) or text format (-text) 
 * all the supported Charsets from your JVM. To use it, run the following command:<br>
 * java -cp WaarpCommon-1.2.7.jar org.waarp.common.transcode.CharsetsUtil [-csv | -html | -text ]<br>
 * <br>
 * It could also be used as a test of transcode outside R66:<br>
 * java -cp WaarpCommon-1.2.7.jar org.waarp.common.transcode.CharsetsUtil -from fromFilename fromCharset -to toFilename toCharset<br>
 * 
 * The current file is not touched and is not marked as moved.
 * 
 * @author Frederic Bregier
 * 
 */
public class TranscodeTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(TranscodeTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public TranscodeTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.TRANSCODE, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		boolean success = false;
		DbTaskRunner runner = session.getRunner();
		String arg = argRule;
		arg = getReplacedValue(arg, argTransfer.split(" "));
		String [] args = arg.split(" ");
		if (args.length < 4) {
			R66Result result = new R66Result(session, false, ErrorCode.Warning,
					runner);
			futureCompletion.setResult(result);
			logger.warn("Not enough argument in Transcode: " + runner.toShortString());
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Not enough argument in Transcode"));
			return;
		}
		String fromCharset = null;
		String toCharset = null;
		String newfilename = null;
		String extension = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-from")) {
				i++;
				if (i < args.length) {
					fromCharset = args[i];
				}
			} else if (args[i].equalsIgnoreCase("-to")) {
				i++;
				if (i < args.length) {
					toCharset = args[i];
				}
			} else if (args[i].equalsIgnoreCase("-newfile")) {
				i++;
				if (i < args.length) {
					newfilename = args[i];
				}
			} else if (args[i].equalsIgnoreCase("-extension")) {
				i++;
				if (i < args.length) {
					extension = args[i];
				}
			}
		}
		if (fromCharset == null || toCharset == null) {
			R66Result result = new R66Result(session, false, ErrorCode.Warning,
					runner);
			futureCompletion.setResult(result);
			logger.warn("Not enough argument in Transcode: " + runner.toShortString());
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Not enough argument in Transcode"));
			return;
		}
		File from = session.getFile().getTrueFile();
		if (newfilename != null) {
			success = CharsetsUtil.transcode(from.getAbsolutePath(), fromCharset, 
					newfilename, toCharset, 
					Configuration.BUFFERSIZEDEFAULT);
		} else if (extension != null) {
			success = CharsetsUtil.transcode(from.getAbsolutePath(), fromCharset, 
					from.getAbsolutePath()+"."+extension, toCharset, 
					Configuration.BUFFERSIZEDEFAULT);
		} else {
			success = CharsetsUtil.transcode(from.getAbsolutePath(), fromCharset, 
					from.getAbsolutePath()+".transcode", toCharset, 
					Configuration.BUFFERSIZEDEFAULT);
		}
		if (success) {
			futureCompletion.setSuccess();
		} else {
			logger.error("Cannot Transcode from " + fromCharset + " to " + toCharset+" with " +
					argRule + ":" + argTransfer + " and " + session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Cannot Transcode file"));
		}
	}

}
