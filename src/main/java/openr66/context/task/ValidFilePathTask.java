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

import java.io.File;

import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.context.task.exception.OpenR66RunnerException;

/**
 * This task validate the File Path according to the follwing argument:<br>
 * - the full path is get from the current file<br>
 * - the arg path is transformed as usual (static and dynamic from information transfer) and should
 * be the beginning of the correct valid path<br>
 * - the full path should begin with one of the result of the transformation (blank separated)<br>
 * <br>
 * For instance "#OUTPATH# #INPATH# #WORKPATH# #ARHCPATH#" will test that the current file is in one
 * of the standard path.
 * 
 * @author Frederic Bregier
 * 
 */
public class ValidFilePathTask extends AbstractTask {
	/**
	 * Internal Logger
	 */
	private static final GgInternalLogger logger = GgInternalLoggerFactory
			.getLogger(ValidFilePathTask.class);

	/**
	 * @param argRule
	 * @param delay
	 * @param argTransfer
	 * @param session
	 */
	public ValidFilePathTask(String argRule, int delay, String argTransfer,
			R66Session session) {
		super(TaskType.VALIDFILEPATH, delay, argRule, argTransfer, session);
	}

	/*
	 * (non-Javadoc)
	 * @see openr66.context.task.AbstractTask#run()
	 */
	@Override
	public void run() {
		String finalname = argRule;
		finalname = R66Dir.normalizePath(
				getReplacedValue(finalname, argTransfer.split(" ")));
		logger.info("Test Valid Path with " + finalname + " from {}", session);
		File from = session.getFile().getTrueFile();
		String curpath = R66Dir.normalizePath(from.getAbsolutePath());
		String[] paths = finalname.split(" ");
		for (String base : paths) {
			if (curpath.startsWith(base)) {
				if (delay > 0) {
					logger.info("Validate File " + curpath + " from " + base + " and\n    " +
							session.toString());
				}
				futureCompletion.setSuccess();
				return;
			}
		}
		if (delay > 0) {
			logger.error("Unvalidate File: " + curpath + "\n    " +
					session.toString());
		}
		futureCompletion.setFailure(new OpenR66RunnerException("File not Validated"));
	}

}
