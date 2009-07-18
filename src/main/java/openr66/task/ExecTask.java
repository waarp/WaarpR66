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

import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;

/**
 * @author Frederic Bregier
 *
 */
public class ExecTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ExecTask.class);
    /**
     * @param argRule
     * @param argTransfer
     * @param session
     */
    public ExecTask(String argRule, String argTransfer, R66Session session) {
        super(TaskType.EXEC, argRule, argTransfer, session);
    }

    /* (non-Javadoc)
     * @see openr66.task.AbstractTask#run()
     */
    @Override
    public void run() {
        /*
         * First apply all replacements and format to argRule from context and argTransfer.
         * Will call exec (from first element of resulting string) with arguments as the following value from the replacements.
         * Return 0 if OK, else 1 for a warning else as an error
         * No change should be done in the filename
         */
        logger.info("Exec with "+this.argRule+":"+this.argTransfer+" and "+this.session);
        String finalname = this.argRule;
        finalname = this.getReplacedValue(finalname, this.argTransfer.split(" "));
        String []args = finalname.split(" ");
        CommandLine commandLine = new CommandLine(args[0]);
        for (int i = 1; i < args.length; i++) {
            commandLine.addArgument(args[i]);
        }
        DefaultExecutor defaultExecutor = new DefaultExecutor();
        PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(null,null);
        defaultExecutor.setStreamHandler(pumpStreamHandler);
        int []correctValues= {0,1};
        defaultExecutor.setExitValues(correctValues);
        int status = -1;
        try {
            status = defaultExecutor.execute(commandLine);
        } catch (ExecuteException e) {
            pumpStreamHandler.stop();
            logger.error("Exception: "+e.getMessage()+" Exec in error with "+commandLine.toString(),e);
            this.futureCompletion.setFailure(e);
            return;
        } catch (IOException e) {
            pumpStreamHandler.stop();
            logger.error("Exception: "+e.getMessage()+" Exec in error with "+commandLine.toString(),e);
            this.futureCompletion.setFailure(e);
            return;
        }
        pumpStreamHandler.stop();
        if (status == 0) {
            this.futureCompletion.setSuccess();
            logger.warn("Exec OK with "+commandLine.toString());
        } else if (status == 1) {
            logger.warn("Exec in warning with "+commandLine.toString());
            this.futureCompletion.setSuccess();
        } else {
            logger.error("Status: "+status+" Exec in error with "+commandLine.toString());
            this.futureCompletion.cancel();
        }
    }

}
