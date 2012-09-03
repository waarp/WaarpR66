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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
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
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-from")) {
				i++;
				fromCharset = args[i];
			} else if (args[i].equalsIgnoreCase("-to")) {
				i++;
				toCharset = args[i];
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
		File to = new File(from.getAbsolutePath()+".transcode");
		FileInputStream fileInputStream;
		try {
			fileInputStream = new FileInputStream(from);
			InputStreamReader reader = new InputStreamReader(fileInputStream, fromCharset);
			FileOutputStream fileOutputStream = new FileOutputStream(to);
			OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream, toCharset);
			char []cbuf = new char[Configuration.BUFFERSIZEDEFAULT];
			int read = reader.read(cbuf);
			while (read > 0) {
				writer.write(cbuf, 0, read);
				read = reader.read(cbuf);
			}
			try {
				reader.close();
			} catch (IOException e) {
			}
			try {
				writer.close();
			} catch (IOException e) {
			}
			success = true;
		} catch (FileNotFoundException e) {
			logger.warn("File not found", e);
		} catch (UnsupportedEncodingException e) {
			logger.warn("Unsupported Encoding", e);
		} catch (IOException e) {
			logger.warn("File IOException", e);
		}
		if (success) {
			session.getRunner().setFileMoved(to.getAbsolutePath(), success);
			futureCompletion.setSuccess();
		} else {
			logger.error("Cannot Transcode from " + fromCharset + " to " + toCharset+" with " +
					argRule + ":" + argTransfer + " and " + session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException(
					"Cannot Transcode file"));
		}
	}

}
