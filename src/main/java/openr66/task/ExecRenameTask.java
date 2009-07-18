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
package openr66.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;

/**
 * @author Frederic Bregier
 *
 */
public class ExecRenameTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ExecRenameTask.class);

    /**
     * @param argRule
     * @param argTransfer
     * @param session
     */
    public ExecRenameTask(String argRule, String argTransfer, R66Session session) {
        super(TaskType.EXECRENAME, argRule, argTransfer, session);
    }

    private class LastLineReader implements Runnable {
        private final BufferedReader reader;

        public String lastLine = null;

        /**
         *
         */
        public LastLineReader(PipedInputStream inputStream) {
            this.reader = new BufferedReader(new InputStreamReader(inputStream));
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.length() > 0) {
                        lastLine = line;
                    }
                }
            } catch (IOException e) {
                lastLine = null;
            }
            try {
                reader.close();
            } catch (IOException e) {
            }
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.task.AbstractTask#run()
     */
    @Override
    public void run() {
        /*
         * First apply all replacements and format to argRule from context and argTransfer.
         * Will call exec (from first element of resulting string) with arguments as the following value from the replacements.
         * Return 0 if OK, else 1 for a warning else as an error.
         * The last line of stdout will be the new name given to the R66File in case of status 0.
         * The previous file should be deleted by the script or will be deleted in case of status 0.
         * If the status is 1, no change is made to the file.
         *
         */
        logger.info("ExecRename with "+this.argRule+":"+this.argTransfer+" and "+this.session);
        String finalname = this.argRule;
        finalname = this.getReplacedValue(finalname, this.argTransfer.split(" "));
        String []args = finalname.split(" ");
        CommandLine commandLine = new CommandLine(args[0]);
        for (int i = 1; i < args.length; i++) {
            commandLine.addArgument(args[i]);
        }
        DefaultExecutor defaultExecutor = new DefaultExecutor();
        PipedInputStream inputStream = new PipedInputStream();
        PipedOutputStream outputStream = null;
        try {
            outputStream = new PipedOutputStream(inputStream);
        } catch (IOException e1) {
            try {
                inputStream.close();
            } catch (IOException e) {
            }
            logger.error("Exception: "+e1.getMessage()+" Exec in error with "+commandLine.toString(),e1);
            this.futureCompletion.setFailure(e1);
            return;
        }
        PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(outputStream,null);
        defaultExecutor.setStreamHandler(pumpStreamHandler);
        int []correctValues= {0,1};
        defaultExecutor.setExitValues(correctValues);
        LastLineReader lastLineReader = new LastLineReader(inputStream);
        Thread thread = new Thread(lastLineReader);
        thread.setDaemon(false);
        thread.setName("ExecRename"+this.session.getRunner().getSpecialId());
        thread.start();
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
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
        }
        pumpStreamHandler.stop();
        String newname = lastLineReader.lastLine;
        if (status == 0) {
            if (newname.indexOf(' ') > 0) {
                logger.warn("Exec returns a multiple string in final line: "+newname);
                args = newname.split(" ");
                newname = args[args.length-1];
            }
            // now test if the previous file was deleted (should be)
            File file = new File(newname);
            if (file.exists()) {
                try {
                    if (this.session.getFile().isFile()) {
                        // not deleted, so do it now
                        try {
                            this.session.getFile().delete();
                        } catch (CommandAbstractException e) {
                            logger.warn("Original File cannot be deleted", e);
                        }
                    }
                } catch (CommandAbstractException e) {
                }
            }
            // now replace the file with the new one
            try {
                this.session.getFile().replaceFilename(newname, true);
            } catch (CommandAbstractException e) {
                logger.warn("Exec in warning with "+commandLine.toString(), e);
            }
            this.futureCompletion.setSuccess();
            logger.warn("Exec OK with "+commandLine.toString()+" returns "+newname);
        } else if (status == 1) {
            logger.warn("Exec in warning with "+commandLine.toString()+" returns "+newname);
            this.futureCompletion.setSuccess();
        } else {
            logger.error("Status: "+status+" Exec in error with "+commandLine.toString()
                    +" returns "+newname);
            this.futureCompletion.cancel();
        }
    }
}
