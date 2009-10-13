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
package openr66.context.task;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * Move the file (without renaming it)
 *
 * @author Frederic Bregier
 *
 */
public class MoveTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(MoveTask.class);

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public MoveTask(String argRule, int delay, String argTransfer,
            R66Session session) {
        super(TaskType.MOVE, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        logger.info("Move with " + argRule + ":" + argTransfer + " and {}",
                session);
        boolean success = false;
        String finalname = argRule +
            R66Dir.getFinalUniqueFilename(
                session.getFile());
        try {
            success = session.getFile().renameTo(finalname, true);
        } catch (CommandAbstractException e) {
            logger.error("Move with " + argRule + ":" + argTransfer + " and " +
                    session, e);
            futureCompletion.setFailure(new OpenR66ProtocolSystemException(e));
            return;
        }
        if (success) {
            session.getRunner().setFileMoved(finalname, success);
            futureCompletion.setSuccess();
        } else {
            logger.error("Cannot Move with " + argRule + ":" + argTransfer +
                    " and " + session);
            futureCompletion.setFailure(new OpenR66ProtocolSystemException(
                    "Cannot move file"));
        }
    }

}
