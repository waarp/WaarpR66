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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import openr66.context.ErrorCode;
import openr66.context.R66Session;

/**
 * This class is for logging or write to an external file some info:<br>
 * - if delay is 0, no echo at all will be done<br>
 * - if delay is 1, will echo some information in the normal log<br>
 * - if delay is 2, will echo some information in the file
 * (last deduced argument will be the full path for the file output)<br>
 * - if delay is 3, will echo both in the normal log and in the file
 * (last deduced argument will be the full path for the file output)<br>
 *
 * @author Frederic Bregier
 *
 */
public class LogTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LogTask.class);

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public LogTask(String argRule, int delay, String argTransfer,
            R66Session session) {
        super(TaskType.LOG, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        String finalValue = argRule;
        finalValue = getReplacedValue(finalValue, argTransfer.split(" "));
        switch (delay) {
            case 0:
                break;
            case 1:
                logger.warn(finalValue+"\n    " + session.toString());
                break;
            case 3:
                logger.warn(finalValue+"\n    " + session.toString());
            case 2:
                String []args = finalValue.split(" ");
                String filename = args[args.length-1];
                File file = new File(filename);
                if (file.getParentFile() == null ||
                    (!file.canWrite())) {
                    // File cannot be written so revert to log
                    session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
                    logger.warn(finalValue+"\n    " + session.toString());
                    futureCompletion.setSuccess();
                    return;
                }
                FileOutputStream outputStream = null;
                try {
                    outputStream = new FileOutputStream(file);
                } catch (FileNotFoundException e) {
                    // File cannot be written so revert to log
                    session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
                    logger.warn(finalValue+"\n    " + session.toString());
                    futureCompletion.setSuccess();
                    return;
                }
                try {
                    outputStream.write(finalValue.getBytes());
                } catch (IOException e) {
                    // File cannot be written so revert to log
                    try {
                        outputStream.close();
                    } catch (IOException e1) {
                    }
                    file.delete();
                    session.getRunner().setErrorExecutionStatus(ErrorCode.Warning);
                    logger.warn(finalValue+"\n    " + session.toString());
                    futureCompletion.setSuccess();
                    return;
                }
                try {
                    outputStream.close();
                } catch (IOException e) {
                }
                break;
            default:
        }
        futureCompletion.setSuccess();
    }

}
