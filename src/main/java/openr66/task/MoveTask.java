/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.task;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
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
     * @param argTransfer
     * @param session
     */
    public MoveTask(String argRule, String argTransfer, R66Session session) {
        super(TaskType.MOVE, argRule, argTransfer, session);
    }

    /* (non-Javadoc)
     * @see openr66.task.AbstractTask#run()
     */
    @Override
    public void run() {
        logger.warn("Move with "+this.argRule+":"+this.argTransfer+" and "+this.session);
        boolean success = false;
        try {
            success =
                this.session.getFile().renameTo(
                        this.argRule+
                        this.session.getDir().getFinalUniqueFilename(this.session.getFile()), true);
        } catch (CommandAbstractException e) {
            this.futureCompletion.setFailure(new OpenR66ProtocolSystemException(e));
            return;
        }
        this.session.getRunner().setFileMoved(success);
        if (success) {
            this.futureCompletion.setSuccess();
        } else {
            this.futureCompletion.setFailure(new OpenR66ProtocolSystemException("Cannot move file"));
        }
    }

}
