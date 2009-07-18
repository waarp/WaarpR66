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

import openr66.filesystem.R66Session;
import openr66.protocol.config.Configuration;
import openr66.protocol.utils.R66Future;

/**
 * @author Frederic Bregier
 *
 */
public abstract class AbstractTask implements Runnable {
    /**
     * Current full path of current filename
     */
    public static final String TRUEFULLPATH = "#TRUEFULLPATH#";
    /**
     * Current filename (change in retrieval part)
     */
    public static final String TRUEFILENAME = "#TRUEFILENAME#";
    /**
     * Original filename (before changing in retrieval part)
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
     * Current or final rank of block
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
     * @param type
     * @param arg
     * @param session
     */
    AbstractTask(TaskType type, String argRule, String argTransfer, R66Session session) {
        this.type = type;
        this.argRule = argRule;
        this.argTransfer = argTransfer;
        this.session = session;
        this.futureCompletion = new R66Future(true);
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
        return this.futureCompletion.isSuccess();
    }
    /**
    *
    * @return the R66Future of completion
    */
   public R66Future getFutureCompletion() {
       return this.futureCompletion;
   }

   /**
    *
    * @param arg as the Format string where
    *   FIXED items will be replaced by context values and
    *   next using argFormat as format second argument
    * @param argFormat as format second argument
    * @return The string with replaced values from context and second argument
    */
   protected String getReplacedValue(String arg, Object []argFormat) {
       String finalname = arg.replace(TRUEFULLPATH, this.session.getFile().getTrueFile().getAbsolutePath());
       finalname = finalname.replace(TRUEFILENAME, this.session.getDir().getFinalUniqueFilename(this.session.getFile()));
       finalname = finalname.replace(ORIGINALFILENAME, this.session.getRequest().getFilename());
       DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
       Date date = new Date();
       finalname = finalname.replace(DATE, dateFormat.format(date));
       dateFormat= new SimpleDateFormat("HHmmss");
       finalname = finalname.replace(HOUR, dateFormat.format(date));
       finalname = finalname.replace(REMOTEHOST, this.session.getAuth().getUser());
       finalname = finalname.replace(LOCALHOST, Configuration.configuration.HOST_ID);
       finalname = finalname.replace(TRANSFERID, Long.toString(this.session.getRunner().getSpecialId()));
       finalname = finalname.replace(RANKTRANSFER, Integer.toString(this.session.getRunner().getRank()));
       finalname = finalname.replace(BLOCKSIZE, Integer.toString(this.session.getBlockSize()));
       finalname = String.format(finalname, argFormat);
       return finalname;
   }
}
