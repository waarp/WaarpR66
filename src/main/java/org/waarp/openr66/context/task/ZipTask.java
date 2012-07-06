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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.tar.ZipUtility;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * TAR task
 * 
 * @author Frederic Bregier
 * 
 */
public class ZipTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(ZipTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public ZipTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.COPY, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		logger.info("ZIP with " + argRule + ":" + argTransfer + ":" + delay + " and {}",
				session);
		String finalname = argRule;
		finalname = getReplacedValue(finalname, argTransfer.split(" "));
		boolean zip = false;
		if ((delay > 2)) {
			if (delay > 3) {
				// list of files: zip finalname where finalname="target file1 file2..."
				String[] args = finalname.split(" ");
				List<File> files = new ArrayList<File>(args.length - 1);
				for (int i = 1; i < args.length; i++) {
					files.add(new File(args[i]));
				}
				zip = ZipUtility.createZipFromFiles(files, args[0]);
			} else {
				// directory: zip finalname where finalname="target directory"
				String[] args = finalname.split(" ");
				zip = ZipUtility.createZipFromDirectory(args[1], args[0], false);
			}
		} else {
			// unzip
			// directory: unzip finalname where finalname="source directory"
			String[] args = finalname.split(" ");
			File zipFile = new File(args[0]);
			File directory = new File(args[1]);
			try {
				ZipUtility.unZip(zipFile, directory);
			} catch (IOException e) {
				logger.warn("Error while unzip", e);
				zip = false;
			}
		}
		if (!zip) {
			logger.error("Zip error with " + argRule + ":" + argTransfer + ":" + delay + " and " +
					session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException("Zip error"));
			return;
		}
		futureCompletion.setSuccess();
	}

}
