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
import openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * @author Frederic Bregier
 *
 */
public class MoveRenameTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(MoveRenameTask.class);

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public MoveRenameTask(String argRule, int delay, String argTransfer, R66Session session) {
        super(TaskType.MOVERENAME, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        boolean success = false;
        /*
         * MOVE avec options dans argRule : "CHAINE de caracteres" qui se
         * verront appliquer : - TRUEFILENAME -> current FILENAME (retrieve) -
         * DATE -> AAAAMMJJ - HOUR -> HHMMSS - puis %s qui sera remplace par les
         * arguments du transfer (transfer information)
         */
        String finalname = argRule;
        finalname = getReplacedValue(finalname, argTransfer
                .split(" "));
        logger.info("Move and Rename to " + finalname + " with " +
                argRule + ":" + argTransfer + " and " + session);
        try {
            success = session.getFile().renameTo(finalname, true);
        } catch (CommandAbstractException e) {
            logger.error("Move and Rename to " + finalname + " with " +
                    argRule + ":" + argTransfer + " and " + session, e);
            futureCompletion
                    .setFailure(new OpenR66ProtocolSystemException(e));
            return;
        }
        session.getRunner().setFileMoved(success);
        if (success) {
            futureCompletion.setSuccess();
        } else {
            logger.error("Cannot Move and Rename to " + finalname + " with " +
                    argRule + ":" + argTransfer + " and " + session);
            futureCompletion
                    .setFailure(new OpenR66ProtocolSystemException(
                            "Cannot move file"));
        }
    }

}
