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

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.utility.GgStringUtils;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import openr66.context.ErrorCode;
import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoSslException;
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
     * Current FILENAME (basename) (change in retrieval part)
     */
    public static final String TRUEFILENAME = "#TRUEFILENAME#";
    /**
     * Current full path of Original FILENAME (as transmitted) (before changing in retrieval part)
     */
    public static final String ORIGINALFULLPATH = "#ORIGINALFULLPATH#";

    /**
     * Original FILENAME (basename) (before changing in retrieval part)
     */
    public static final String ORIGINALFILENAME = "#ORIGINALFILENAME#";

    /**
     * Size of the current FILE
     */
    public static final String FILESIZE = "#FILESIZE#";

    /**
     * Current full path of current RULE
     */
    public static final String RULE = "#RULE#";

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
     * Remote host address
     */
    public static final String REMOTEHOSTADDR = "#REMOTEHOSTADDR#";

    /**
     * Local host id
     */
    public static final String LOCALHOST = "#LOCALHOST#";

    /**
     * Local host address
     */
    public static final String LOCALHOSTADDR = "#LOCALHOSTADDR#";

    /**
     * Transfer id
     */
    public static final String TRANSFERID = "#TRANSFERID#";

    /**
     * Requester Host
     */
    public static final String REQUESTERHOST = "#REQUESTERHOST#";

    /**
     * Requested Host
     */
    public static final String REQUESTEDHOST = "#REQUESTEDHOST#";

    /**
     * Full Transfer id (TRANSFERID_REQUESTERHOST_REQUESTEDHOST)
     */
    public static final String FULLTRANSFERID = "#FULLTRANSFERID#";

    /**
     * Current or final RANK of block
     */
    public static final String RANKTRANSFER = "#RANKTRANSFER#";

    /**
     * Block size used
     */
    public static final String BLOCKSIZE = "#BLOCKSIZE#";

    /**
     * IN Path used
     */
    public static final String INPATH = "#INPATH#";

    /**
     * OUT Path used
     */
    public static final String OUTPATH = "#OUTPATH#";

    /**
     * WORK Path used
     */
    public static final String WORKPATH = "#WORKPATH#";

    /**
     * ARCH Path used
     */
    public static final String ARCHPATH = "#ARCHPATH#";

    /**
     * HOME Path used
     */
    public static final String HOMEPATH = "#HOMEPATH#";
    /**
     * Last Current Error Message
     */
    public static final String ERRORMSG = "#ERRORMSG#";
    /**
     * Last Current Error Code
     */
    public static final String ERRORCODE = "#ERRORCODE#";
    /**
     * Last Current Error Code in Full String
     */
    public static final String ERRORSTRCODE = "#ERRORSTRCODE#";
    /**
     * If specified, no Wait for Task Validation (default is wait)
     */
    public static final String NOWAIT = "#NOWAIT#";
    /**
     * If specified, use the LocalExec Daemon specified in the global configuration
     * (default no usage of LocalExec)
     */
    public static final String LOCALEXEC = "#LOCALEXEC#";
    
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
     * Do we wait for a validation of the task ?
     * Default = True
     */
    boolean waitForValidation = true;
    /**
     * Do we need to use LocalExec for an Exec Task ?
     * Default = False
     */
    boolean useLocalExec = false;
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
        futureCompletion.awaitUninterruptibly();
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
        StringBuilder builder = new StringBuilder(arg);
        // check NOWAIT and LOCALEXEC
        if (arg.contains(NOWAIT)) {
            waitForValidation = false;
            GgStringUtils.replaceAll(builder, NOWAIT, "");
        }
        if (arg.contains(LOCALEXEC)) {
            useLocalExec = true;
            GgStringUtils.replaceAll(builder, LOCALEXEC, "");
        }
        File trueFile = null;
        if (session.getFile() != null) {
            trueFile = session.getFile().getTrueFile();
        }
        if (trueFile != null) {
            GgStringUtils.replaceAll(builder, TRUEFULLPATH, trueFile.getAbsolutePath());
            GgStringUtils.replaceAll(builder, TRUEFILENAME, R66Dir
                    .getFinalUniqueFilename(session.getFile()));
            GgStringUtils.replaceAll(builder, FILESIZE, Long.toString(trueFile.length()));
        } else {
            GgStringUtils.replaceAll(builder, TRUEFULLPATH, "nofile");
            GgStringUtils.replaceAll(builder, TRUEFILENAME, "nofile");
            GgStringUtils.replaceAll(builder, FILESIZE, "0");
        }
        GgStringUtils.replaceAll(builder, ORIGINALFULLPATH, session.getRunner()
                .getOriginalFilename());
        GgStringUtils.replaceAll(builder, ORIGINALFILENAME, R66File.getBasename(session.getRunner()
                .getOriginalFilename()));
        GgStringUtils.replaceAll(builder, RULE, session.getRunner()
                .getRuleId());
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        GgStringUtils.replaceAll(builder, DATE, dateFormat.format(date));
        dateFormat = new SimpleDateFormat("HHmmss");
        GgStringUtils.replaceAll(builder, HOUR, dateFormat.format(date));
        if (session.getAuth() != null) {
            GgStringUtils.replaceAll(builder, REMOTEHOST, session.getAuth().getUser());
            try {
                GgStringUtils.replaceAll(builder, LOCALHOST,
                        Configuration.configuration.getHostId(session.getAuth().isSsl()));
            } catch (OpenR66ProtocolNoSslException e) {
                // replace by standard name
                GgStringUtils.replaceAll(builder, LOCALHOST,
                        Configuration.configuration.HOST_ID);
            }
        }
        if (session.getRemoteAddress() != null) {
            GgStringUtils.replaceAll(builder, REMOTEHOSTADDR, session.getRemoteAddress().toString());
            GgStringUtils.replaceAll(builder, LOCALHOSTADDR, session.getLocalAddress().toString());
        } else {
            GgStringUtils.replaceAll(builder, REMOTEHOSTADDR, "unknown");
            GgStringUtils.replaceAll(builder, LOCALHOSTADDR, "unknown");
        }
        GgStringUtils.replaceAll(builder, TRANSFERID, Long.toString(session
                .getRunner().getSpecialId()));
        String requester = session.getRunner().getRequester();
        GgStringUtils.replaceAll(builder, REQUESTERHOST, requester);
        String requested = session.getRunner().getRequested();
        GgStringUtils.replaceAll(builder, REQUESTEDHOST, requested);
        GgStringUtils.replaceAll(builder, FULLTRANSFERID, session
                .getRunner().getSpecialId()+"_"+requester+"_"+requested);
        GgStringUtils.replaceAll(builder, RANKTRANSFER, Integer.toString(session
                .getRunner().getRank()));
        GgStringUtils.replaceAll(builder, BLOCKSIZE, Integer.toString(session
                .getBlockSize()));
        R66Dir dir = new R66Dir(session);
        try {
            dir.changeDirectory(session.getRunner().getRule().recvPath);
            GgStringUtils.replaceAll(builder, INPATH, dir.getFullPath());
        } catch (CommandAbstractException e) {
        }
        dir = new R66Dir(session);
        try {
            dir.changeDirectory(session.getRunner().getRule().sendPath);
            GgStringUtils.replaceAll(builder, OUTPATH, dir.getFullPath());
        } catch (CommandAbstractException e) {
        }
        dir = new R66Dir(session);
        try {
            dir.changeDirectory(session.getRunner().getRule().workPath);
            GgStringUtils.replaceAll(builder, WORKPATH, dir.getFullPath());
        } catch (CommandAbstractException e) {
        }
        dir = new R66Dir(session);
        try {
            dir.changeDirectory(session.getRunner().getRule().archivePath);
            GgStringUtils.replaceAll(builder, ARCHPATH, dir.getFullPath());
        } catch (CommandAbstractException e) {
        }
        GgStringUtils.replaceAll(builder, HOMEPATH, Configuration.configuration.baseDirectory);
        if (session.getLocalChannelReference() == null) {
            GgStringUtils.replaceAll(builder, ERRORMSG, "NoError");
            GgStringUtils.replaceAll(builder, ERRORCODE, "-");
            GgStringUtils.replaceAll(builder, ERRORSTRCODE, ErrorCode.Unknown.name());
        } else {
            try {
                GgStringUtils.replaceAll(builder, ERRORMSG, session.getLocalChannelReference().getErrorMessage());
            } catch (NullPointerException e) {
                GgStringUtils.replaceAll(builder, ERRORMSG, "NoError");
            }
            try {
                GgStringUtils.replaceAll(builder, ERRORCODE, session.getLocalChannelReference().getCurrentCode().getCode());
            } catch (NullPointerException e) {
                GgStringUtils.replaceAll(builder, ERRORCODE, "-");
            }
            try {
                GgStringUtils.replaceAll(builder, ERRORSTRCODE, session.getLocalChannelReference().getCurrentCode().name());
            } catch (NullPointerException e) {
                GgStringUtils.replaceAll(builder, ERRORSTRCODE, ErrorCode.Unknown.name());
            }
        }
        String finalname = String.format(builder.toString(), argFormat);
        return finalname;
    }
}
