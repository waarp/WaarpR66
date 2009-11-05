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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import openr66.client.SubmitTransfer;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.utils.R66Future;

/**
 * Transfer task:<br>
 *
 * Result of arguments will be as r66send command but in that order: <br>
 * "-file filepath -to requestedHost -rule rule -info information"<br>
 * where information can include blank char too.
 *
 * @author Frederic Bregier
 *
 */
public class TransferTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(TransferTask.class);

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public TransferTask(String argRule, int delay, String argTransfer,
            R66Session session) {
        super(TaskType.TRANSFER, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        logger.info("Transfer with " + argRule + ":" + argTransfer + " and {}",
                session);
        String finalname = argRule;
        finalname = getReplacedValue(finalname, argTransfer.split(" "));
        String[] args = finalname.split(" ");
        if (args.length < 8) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Not enough argument in Transfer"));
        }
        String filepath = args[1];
        String requested = args[3];
        String rule = args[5];
        String information = args[7];
        for (int i = 8; i < args.length; i++) {
            information += " "+args[i];
        }
        R66Future future = new R66Future(true);
        SubmitTransfer transaction = new SubmitTransfer(future,
                requested, filepath, rule, information, false, 0);
        transaction.run();
        future.awaitUninterruptibly();
        futureCompletion.setResult(future.getResult());
        DbTaskRunner runner = future.getResult().runner;
        if (future.isSuccess()) {
            logger.warn("Prepare transfer in\n    SUCCESS\n    "+runner.toShortString()+
                            "\n    <REMOTE>"+requested+"</REMOTE>");
            futureCompletion.setSuccess();
        } else {
            if (runner != null) {
                if (future.getCause() == null) {
                    futureCompletion.cancel();
                } else {
                    futureCompletion.setFailure(future.getCause());
                }
                logger.error("Prepare transfer in\n    FAILURE\n     "+runner.toShortString()+
                            "\n    <REMOTE>"+requested+"</REMOTE>", future.getCause());
            } else {
                if (future.getCause() == null) {
                    futureCompletion.cancel();
                } else {
                    futureCompletion.setFailure(future.getCause());
                }
                logger.error("Prepare transfer in\n    FAILURE without any runner back", future.getCause());
            }
        }
    }

}
