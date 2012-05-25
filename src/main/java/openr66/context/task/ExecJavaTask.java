/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.context.task;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;


/**
 * Execute a Java command through Class.forName call
 *
 *
 * @author Frederic Bregier
 *
 */
public class ExecJavaTask extends AbstractTask {

    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ExecJavaTask.class);

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public ExecJavaTask(String argRule, int delay, String argTransfer,
            R66Session session) {
        super(TaskType.EXECJAVA, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        /*
         * First apply all replacements and format to argRule from context and
         * argTransfer. Will call exec (from first element of resulting string)
         * with arguments as the following value from the replacements. Return 0
         * if OK, else 1 for a warning else as an error. No change should be done
         * in the FILENAME
         */
        logger.debug("Exec with " + argRule + ":" + argTransfer + " and {}",
                session);
        String finalname = argRule;
        if (argTransfer != null) {
            finalname = getReplacedValue(finalname, argTransfer.split(" "));
        }
        // First get the Class Name
        String[] args = finalname.split(" ");
        String className = args[0];
        R66Runnable runnable = null;
        try {
            runnable = (R66Runnable) Class.forName(className).newInstance();
        } catch (Exception e) {
            logger.error("ExecJava command is not available: " + className, e);
            R66Result result = new R66Result(session, false,
                    ErrorCode.CommandNotFound, session.getRunner());
            futureCompletion.setResult(result);
            futureCompletion.cancel();
            return;
        }
        runnable.setArgs(this.session, this.waitForValidation, this.useLocalExec, 
                this.delay, args);
        if (! waitForValidation) {
            // Do not wait for validation
            futureCompletion.setSuccess();
            logger.info("Exec will start but no WAIT with {}", runnable);
        }
        int status = -1;
        if (waitForValidation && delay <= 0) {
            runnable.run();
            status = runnable.getFinalStatus();
        } else {
            ExecutorService executorService = Executors.newFixedThreadPool(1);
            executorService.execute(runnable);
            try {
                Thread.yield();
                executorService.shutdown();
                if (waitForValidation) {
                    if (delay > 0) {
                        if (! executorService.awaitTermination(delay, TimeUnit.MILLISECONDS)) {
                            executorService.shutdownNow();
                            logger.error("Exec is in Time Out");
                            status = -1;
                        } else {
                            status = runnable.getFinalStatus();
                        }
                    } else {
                        while (!executorService.awaitTermination(30, TimeUnit.SECONDS)) ;
                        status = runnable.getFinalStatus();
                    }
                } else {
                    while (!executorService.awaitTermination(30, TimeUnit.SECONDS)) ;
                    status = runnable.getFinalStatus();
                }
            } catch (InterruptedException e) {
                logger.error("Status: " + e.getMessage() + "\n\t Exec in error with " +
                        runnable);
                if (waitForValidation) {
                    futureCompletion.cancel();
                }
                return;
            }
        }
        if (status == 0) {
            if (waitForValidation) {
                futureCompletion.setSuccess();
            }
            logger.info("Exec OK with {}", runnable);
        } else if (status == 1) {
            logger.warn("Exec in warning with " + runnable);
            if (waitForValidation) {
                futureCompletion.setSuccess();
            }
        } else {
            logger.error("Status: " + status + " Exec in error with " +
                    runnable);
            if (waitForValidation) {
                futureCompletion.cancel();
            }
        }
    }

}
