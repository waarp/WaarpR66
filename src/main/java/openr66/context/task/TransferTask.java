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
 * Transfer task:
 *
 * Result of arguments will be: "filepath requestedHost rule information"\n
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
        if (args.length < 4) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Not enough argument in Transfer"));
        }
        String filepath = args[0];
        String requested = args[1];
        String rule = args[2];
        String information = args[3];
        for (int i = 4; i < args.length; i++) {
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
