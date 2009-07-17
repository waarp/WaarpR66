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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;
import openr66.protocol.config.Configuration;
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

    private static final String TRUEFILENAME = "#TRUEFILENAME#";
    private static final String ORIGINALFILENAME = "#ORIGINALFILENAME#";
    private static final String DATE = "#DATE#";
    private static final String HOUR = "#HOUR#";
    private static final String REMOTEHOST = "#REMOTEHOST#";
    private static final String LOCALHOST = "#LOCALHOST#";

    /**
     * @param argRule
     * @param argTransfer
     * @param session
     */
    public MoveRenameTask(String argRule, String argTransfer, R66Session session) {
        super(TaskType.MOVERENAME, argRule, argTransfer, session);
    }

    /* (non-Javadoc)
     * @see openr66.task.AbstractTask#run()
     */
    @Override
    public void run() {
        boolean success = false;
        /*
         * MOVE avec options dans argRule : "CHAINE de caracteres" qui se verront appliquer :
        - TRUEFILENAME -> current filename (retrieve)
        - DATE -> AAAAMMJJ
        - HOUR -> HHMMSS
        - puis %s qui sera remplace par les arguments du transfer (transfer information)
         */
        String finalname = this.argRule;
        finalname = finalname.replaceFirst(TRUEFILENAME, this.session.getDir().getFinalUniqueFilename(this.session.getFile()));
        finalname = finalname.replaceFirst(ORIGINALFILENAME, this.session.getRequest().getFilename());
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        finalname = finalname.replaceFirst(DATE, dateFormat.format(date));
        dateFormat= new SimpleDateFormat("HHMMss");
        finalname = finalname.replaceFirst(HOUR, dateFormat.format(date));
        finalname = finalname.replaceFirst(REMOTEHOST, this.session.getAuth().getUser());
        finalname = finalname.replaceFirst(LOCALHOST, Configuration.configuration.HOST_ID);
        String []args = this.argTransfer.split(" ");
        finalname = String.format(finalname, (Object [])args);
        logger.warn("Move and Rename to "+finalname+" with "+this.argRule+":"+this.argTransfer+" and "+this.session);
        try {
            success =
                this.session.getFile().renameTo(
                        finalname, true);
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
