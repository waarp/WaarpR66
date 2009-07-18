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

import java.io.File;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

/**
 * @author Frederic Bregier
 *
 */
public class CopyRenameTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(CopyRenameTask.class);

    /**
     * @param argRule
     * @param argTransfer
     * @param session
     */
    public CopyRenameTask(String argRule, String argTransfer, R66Session session) {
        super(TaskType.COPYRENAME, argRule, argTransfer, session);
    }

    /* (non-Javadoc)
     * @see openr66.task.AbstractTask#run()
     */
    @Override
    public void run() {
        /*
         * COPY avec options dans argRule : "CHAINE de caracteres" qui se verront appliquer :
        - TRUEFILENAME -> current filename (retrieve)
        - DATE -> AAAAMMJJ
        - HOUR -> HHMMSS
        - puis %s qui sera remplace par les arguments du transfer (transfer information)
         */
        String finalname = this.argRule;
        finalname = this.getReplacedValue(finalname, this.argTransfer.split(" "));
        logger.warn("Copy and Rename to "+finalname+" with "+this.argRule+":"+this.argTransfer+" and "+this.session);
        File from = this.session.getFile().getTrueFile();
        File to = new File(finalname);
        try {
            FileUtils.copy(from, to, false, false);
        } catch (OpenR66ProtocolSystemException e1) {
            this.futureCompletion.setFailure(new OpenR66ProtocolSystemException(e1));
            return;
        }
        this.futureCompletion.setSuccess();
    }

}
