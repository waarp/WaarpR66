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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import openr66.context.R66Session;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.utils.R66Future;

/**
 * Abstract implementation of task
 *
 * @author Frederic Bregier
 *
 */
public abstract class AbstractTask implements Runnable {
    /**
     * Current full path of current FILENAME
     */
    public static final String TRUEFULLPATH = "#TRUEFULLPATH#";

    /**
     * Current FILENAME (change in retrieval part)
     */
    public static final String TRUEFILENAME = "#TRUEFILENAME#";

    /**
     * Original FILENAME (before changing in retrieval part)
     */
    public static final String ORIGINALFILENAME = "#ORIGINALFILENAME#";

    /**
     * Date in yyyyMMdd format
     */
    public static final String DATE = "#DATE#";

    /**
     * Hour in HHmmss format
     */
    public static final String HOUR = "#HOUR#";

    /**
     * Remote host id (if not the initiator of the call)
     */
    public static final String REMOTEHOST = "#REMOTEHOST#";

    /**
     * Local host id
     */
    public static final String LOCALHOST = "#LOCALHOST#";

    /**
     * Transfer id
     */
    public static final String TRANSFERID = "#TRANSFERID#";

    /**
     * Current or final RANK of block
     */
    public static final String RANKTRANSFER = "#RANKTRANSFER#";

    /**
     * Block size used
     */
    public static final String BLOCKSIZE = "#BLOCKSIZE#";

    /**
     * Type of operation
     */
    final TaskType type;

    /**
     * Argument from Rule
     */
    final String argRule;

    /**
     * Delay from Rule (if applicable)
     */
    final int delay;

    /**
     * Argument from Transfer
     */
    final String argTransfer;

    /**
     * Current session
     */
    final R66Session session;

    /**
     * R66Future of completion
     */
    final R66Future futureCompletion;

    /**
     * Constructor
     *
     * @param type
     * @param delay
     * @param arg
     * @param session
     */
    AbstractTask(TaskType type, int delay, String argRule, String argTransfer,
            R66Session session) {
        this.type = type;
        this.delay = delay;
        this.argRule = argRule;
        this.argTransfer = argTransfer;
        this.session = session;
        futureCompletion = new R66Future(true);
    }

    /**
     * This is the only interface to execute an operator.
     */
    abstract public void run();

    /**
     *
     * @return True if the operation is in success status
     */
    public boolean isSuccess() {
        return futureCompletion.isSuccess();
    }

    /**
     *
     * @return the R66Future of completion
     */
    public R66Future getFutureCompletion() {
        return futureCompletion;
    }

    /**
     *
     * @param arg
     *            as the Format string where FIXED items will be replaced by
     *            context values and next using argFormat as format second
     *            argument; this arg comes from the rule itself
     * @param argFormat
     *            as format second argument; this argFormat comes from the transfer
     *            Information itself
     * @return The string with replaced values from context and second argument
     */
    protected String getReplacedValue(String arg, Object[] argFormat) {
        String finalname = arg.replace(TRUEFULLPATH, session.getFile()
                .getTrueFile().getAbsolutePath());
        finalname = finalname.replace(TRUEFILENAME, session.getDir()
                .getFinalUniqueFilename(session.getFile()));
        finalname = finalname.replace(ORIGINALFILENAME, session.getRunner()
                .getOriginalFilename());
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        finalname = finalname.replace(DATE, dateFormat.format(date));
        dateFormat = new SimpleDateFormat("HHmmss");
        finalname = finalname.replace(HOUR, dateFormat.format(date));
        finalname = finalname.replace(REMOTEHOST, session.getAuth().getUser());
        finalname = finalname.replace(LOCALHOST,
                Configuration.configuration.HOST_ID);
        finalname = finalname.replace(TRANSFERID, Long.toString(session
                .getRunner().getSpecialId()));
        finalname = finalname.replace(RANKTRANSFER, Integer.toString(session
                .getRunner().getRank()));
        finalname = finalname.replace(BLOCKSIZE, Integer.toString(session
                .getBlockSize()));
        finalname = String.format(finalname, argFormat);
        return finalname;
    }
}
