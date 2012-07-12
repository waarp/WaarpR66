/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.context.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.tar.TarUtility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import openr66.context.R66Session;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * TAR task
 * 
 * @author Frederic Bregier
 * 
 */
public class TarTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final GgInternalLogger logger = GgInternalLoggerFactory
			.getLogger(TarTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public TarTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.TAR, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		logger.info("TAR with " + argRule + ":" + argTransfer + ":" + delay + " and {}",
				session);
		String finalname = argRule;
		finalname = getReplacedValue(finalname, argTransfer.split(" "));
		boolean tar = false;
		if ((delay > 2)) {
			if (delay > 3) {
				// list of files: tar finalname where finalname="target file1 file2..."
				String[] args = finalname.split(" ");
				List<File> files = new ArrayList<File>(args.length - 1);
				for (int i = 1; i < args.length; i++) {
					files.add(new File(args[i]));
				}
				tar = TarUtility.createTarFromFiles(files, args[0]);
			} else {
				// directory: tar finalname where finalname="target directory"
				String[] args = finalname.split(" ");
				tar = TarUtility.createTarFromDirectory(args[1], args[0], false);
			}
		} else {
			// untar
			// directory: untar finalname where finalname="source directory"
			String[] args = finalname.split(" ");
			File tarFile = new File(args[0]);
			File directory = new File(args[1]);
			try {
				TarUtility.unTar(tarFile, directory);
			} catch (IOException e) {
				logger.warn("Error while untar", e);
				tar = false;
			}
		}
		if (!tar) {
			logger.error("Tar error with " + argRule + ":" + argTransfer + ":" + delay + " and " +
					session);
			futureCompletion.setFailure(new OpenR66ProtocolSystemException("Tar error"));
			return;
		}
		futureCompletion.setSuccess();
	}

}
