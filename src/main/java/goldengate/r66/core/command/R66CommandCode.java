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
package goldengate.r66.core.command;

import goldengate.common.command.CommandInterface;
import goldengate.common.file.SessionInterface;
//XXX TODO FIXME
import goldengate.ftp.core.command.FtpCommandCode;
import goldengate.ftp.core.command.internal.ConnectionCommand;
import goldengate.ftp.core.command.internal.IncorrectCommand;
import goldengate.ftp.core.command.internal.UnimplementedCommand;
import goldengate.ftp.core.command.internal.UnknownCommand;

/**
 * This class must reassemble all the commands that could be implemented. The
 * comment says the object of the command and the kind of returned codes that
 * could follow this command.<br>
 * <br>
 * Command structure:<br>
 * Main class<br>
 * Previous Valid Command (null means all are valid)<br>
 * Next Valid Commands (none means all are valid)<br>
 *
 * @author Frederic Bregier
 *
 */
public enum R66CommandCode {
    // XXX CONNECTION
    /**
     * Command to simulate the beginning of a connection in order to force the
     * authentication step.<br>
     *
     *
     * 120->220<br>
     * 220<br>
     * 421<br>
     */
    Connection(
            ConnectionCommand.class,
            null,
            goldengate.ftp.core.command.access.USER.class),
    // XXX ACCESS CONTROL COMMAND
    /**
     * The argument field is a Telnet string identifying the user. The user
     * identification is that which is required by the server for access to its
     * file system. This command will normally be the first command transmitted
     * by the user after the control connections are made (some servers may
     * require this). Additional identification information in the form of a
     * password and/or an account command may also be required by some servers.
     * Servers may allow a new USER command to be entered at any point in order
     * to change the access control and/or accounting information. This has the
     * effect of flushing any user, password, and account information already
     * supplied and beginning the login sequence again. All transfer parameters
     * are unchanged and any file transfer in progress is completed under the
     * old access control parameters.<br>
     *
     * 230<br>
     * 530<br>
     * 500, 501, 421<br>
     * 331, 332<br>
     */
    USER(goldengate.ftp.core.command.access.USER.class, ConnectionCommand.class),
    /**
     * The argument field is a Telnet string specifying the user's password.
     * This command must be immediately preceded by the user name command, and,
     * for some sites, completes the user's identification for access control.
     * Since password information is quite sensitive, it is desirable in general
     * to "mask" it or suppress typeout. It appears that the server has no
     * foolproof way to achieve this. It is therefore the responsibility of the
     * user-FTP process to hide the sensitive password information.<br>
     *
     *
     * 230<br>
     * 202<br>
     * 530<br>
     * 500, 501, 503, 421<br>
     * 332<br>
     */
    PASS(goldengate.ftp.core.command.access.PASS.class, null),
    /**
     * The argument field is a Telnet string identifying the user's account. The
     * command is not necessarily related to the USER command, as some sites may
     * require an account for login and others only for specific access, such as
     * storing files. In the latter case the command may arrive at any time.<br>
     * <br>
     *
     * There are reply codes to differentiate these cases for the automation:
     * when account information is required for login, the response to a
     * successful PASSword command is reply code 332. On the other hand, if
     * account information is NOT required for login, the reply to a successful
     * PASSword command is 230; and if the account information is needed for a
     * command issued later in the dialogue, the server should return a 332 or
     * 532 reply depending on whether it stores (pending receipt of the ACCounT
     * command) or discards the command, respectively.<br>
     *
     *
     * 230<br>
     * 202<br>
     * 530<br>
     * 500, 501, 503, 421<br>
     */
    ACCT(goldengate.ftp.core.command.access.ACCT.class, null),
    /**
     * This command terminates a USER and if file transfer is not in progress,
     * the server closes the control connection. If file transfer is in
     * progress, the connection will remain open for result response and the
     * server will then close it. If the user-process is transferring files for
     * several USERs but does not wish to close and then reopen connections for
     * each, then the REIN command should be used instead of QUIT.<br>
     * <br>
     *
     * An unexpected close on the control connection will cause the server to
     * take the effective action of an abort (ABOR) and a logout (QUIT).<br>
     *
     *
     * 221<br>
     * 500<br>
     */
    QUIT(goldengate.ftp.core.command.access.QUIT.class, null),
    /**
     * This command does not affect any parameters or previously entered
     * commands. It specifies no action other than that the server send an OK
     * reply.<br>
     *
     *
     * 200<br>
     * 500 421<br>
     */
    NOOP(goldengate.ftp.core.command.info.NOOP.class, null),
    // XXX GLOBAL OPERATION
    /**
     * Unknown Command from control network<br>
     * Always return 500<br>
     */
    Unknown(UnknownCommand.class, null),
    /**
     * Unimplemented command<br>
     * Always return 502<br>
     */
    Unimplemented(UnimplementedCommand.class, null),
    /**
     * Bad sequence of commands<br>
     * Always return 503<br>
     */
    IncorrectSequence(IncorrectCommand.class, null),

    // XXX INTERNAL FUNCTION

    /**
     * Shutdown command (internal password protected command).<br>
     * Shutdown the FTP service<br>
     */
    INTERNALSHUTDOWN(
            goldengate.ftp.core.command.internal.INTERNALSHUTDOWN.class,
            null),
    /**
     * Change the Limit of the global bandwidth.<br>
     * No argument reset to default, 1 argument change both write and read to
     * same value, 2 arguments stand for write then read limit.<br>
     * Limit is written in byte/s. Example: "LIMITBANDWIDTH 104857600 104857600"
     * stands for 100MB/s limitation globaly.<br>
     * -1 means no limit
     */
    LIMITBANDWIDTH(
            goldengate.ftp.core.command.internal.LIMITBANDWIDTH.class,
            null);


    /**
     * The Class that implements this command
     */
    public Class<? extends CommandInterface> command;

    /**
     * Previous positive class that must precede this command (null means any)
     */
    public Class<? extends CommandInterface> previousValid;

    /**
     * Next valids class that could follow this command (null means any)
     */
    public Class<?>[] nextValids;

    private R66CommandCode(Class<? extends CommandInterface> command,
            Class<? extends CommandInterface> previousValid,
            Class<?>... nextValids) {
        this.command = command;
        this.previousValid = previousValid;
        this.nextValids = nextValids;
    }

    /**
     * Get the corresponding AbstractCommand object from the line received from
     * the client associated with the handler
     *
     * @param session
     * @param line
     * @return the AbstractCommand from the line received from the client
     */
    public static CommandInterface getFromLine(SessionInterface session, String line) {
        R66CommandCode ftpCommandCode = null;
        String newline = line;
        if (newline == null) {
            ftpCommandCode = R66CommandCode.Unknown;
            newline = "";
        }
        String command = null;
        String arg = null;
        if (newline.indexOf(' ') == -1) {
            command = newline;
            arg = null;
        } else {
            command = newline.substring(0, newline.indexOf(' '));
            arg = newline.substring(newline.indexOf(' ') + 1);
            if (arg.length() == 0) {
                arg = null;
            }
        }
        String COMMAND = command.toUpperCase();
        try {
            ftpCommandCode = R66CommandCode.valueOf(COMMAND);
        } catch (IllegalArgumentException e) {
            ftpCommandCode = R66CommandCode.Unknown;
        }
        CommandInterface abstractCommand;
        try {
            abstractCommand = ftpCommandCode.command.newInstance();
        } catch (InstantiationException e) {
            abstractCommand = new UnknownCommand();
            abstractCommand.setArgs(session, COMMAND, arg, Unknown);
            return abstractCommand;
        } catch (IllegalAccessException e) {
            abstractCommand = new UnknownCommand();
            abstractCommand.setArgs(session, COMMAND, arg, Unknown);
            return abstractCommand;
        }
        abstractCommand.setArgs(session, COMMAND, arg, ftpCommandCode);
        return abstractCommand;
    }

    /**
     * True if the command is a Store like operation (APPE, STOR, STOU, ...)
     *
     * @param command
     * @return True if the command is a Store like operation (APPE, STOR, STOU,
     *         ...)
     */
    public static boolean isStoreLikeCommand(FtpCommandCode command) {
        return false; //XXX TODO FIXME command == APPE || command == STOR || command == STOU;
    }

    /**
     * True if the command is a Retrieve like operation (RETR, ...)
     *
     * @param command
     * @return True if the command is a Retrieve like operation (RETR, ...)
     */
    public static boolean isRetrLikeCommand(FtpCommandCode command) {
        return false; //XXX TODO FIXME command == RETR;
    }

    /**
     * True if the command is a List like operation (LIST, NLST, MLSD, MLST,
     * ...)
     *
     * @param command
     * @return True if the command is a List like operation (LIST, NLST, MLSD,
     *         MLST, ...)
     */
    public static boolean isListLikeCommand(FtpCommandCode command) {
        return false; //XXX TODO FIXME command == LIST || command == NLST || command == MLSD || command == MLST;
    }

    /**
     * True if the command is a special operation (QUIT, ABOR, NOOP, STAT, ...)
     *
     * @param command
     * @return True if the command is a special operation (QUIT, ABOR, NOOP,
     *         STAT, ...)
     */
    public static boolean isSpecialCommand(FtpCommandCode command) {
        return false; //XXX TODO FIXME command == QUIT || command == ABOR || command == NOOP || command == STAT;
    }

    /**
     * True if the command is an extension operation (XMD5, XCRC, XSHA1, ...)
     *
     * @param command
     * @return True if the command is an extension operation (XMD5, XCRC, XSHA1,
     *         ...)
     */
    public static boolean isExtensionCommand(FtpCommandCode command) {
        return false; //XXX TODO FIXME command == XMD5 || command == XCRC || command == XSHA1 ||
        //XXX TODO FIXME command == INTERNALSHUTDOWN || command == LIMITBANDWIDTH;
    }

    @Override
    public String toString() {
        return name();
    }
}
