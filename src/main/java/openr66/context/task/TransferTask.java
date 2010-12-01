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
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.utils.R66Future;

/**
 * Transfer task:<br>
 *
 * Result of arguments will be as r66send command.<br>
 * Format is like r66send command in any order except "-info" which should be
 * the last item:<br>
 * "-file filepath -to requestedHost -rule rule [-md5] [-info information]"<br>
 * <br>
 * INFO is the only one field that can contains blank character.<br>
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
        if (args.length < 6) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Not enough argument in Transfer"));
        }
        String filepath = null;
        String requested = null;
        String rule = null;
        String information = null;
        boolean isMD5 = false;
        int blocksize = Configuration.configuration.BLOCKSIZE;
        for (int i = 0; i < args.length; i ++) {
            if (args[i].equalsIgnoreCase("-to")) {
                i ++;
                requested = args[i];
            } else if (args[i].equalsIgnoreCase("-file")) {
                i ++;
                filepath = args[i];
            } else if (args[i].equalsIgnoreCase("-rule")) {
                i ++;
                rule = args[i];
            } else if (args[i].equalsIgnoreCase("-info")) {
                i ++;
                information = args[i];
                i ++;
                while (i < args.length) {
                    information += " " + args[i];
                    i ++;
                }
            } else if (args[i].equalsIgnoreCase("-md5")) {
                isMD5 = true;
            } else if (args[i].equalsIgnoreCase("-block")) {
                i ++;
                blocksize = Integer.parseInt(args[i]);
                if (blocksize < 100) {
                    logger.warn("Block size is too small: " + blocksize);
                    blocksize = Configuration.configuration.BLOCKSIZE;
                }
            }
        }
        if (information == null) {
            information = "noinfo";
        }
        R66Future future = new R66Future(true);
        SubmitTransfer transaction = new SubmitTransfer(future,
                requested, filepath, rule, information, isMD5, blocksize, DbConstant.ILLEGALVALUE);
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
