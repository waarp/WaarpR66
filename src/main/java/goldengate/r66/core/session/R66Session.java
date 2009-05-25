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
package goldengate.r66.core.session;

import goldengate.common.command.CommandInterface;
import goldengate.common.command.ReplyCode;
import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.file.AuthInterface;
import goldengate.common.file.DirInterface;
import goldengate.common.file.FileParameterInterface;
import goldengate.common.file.Restart;
import goldengate.common.file.SessionInterface;
import goldengate.r66.core.command.AbstractCommand;
import goldengate.r66.core.control.BusinessHandler;
import goldengate.r66.core.control.NetworkHandler;

/**
 * @author Frederic Bregier
 *
 */
public class R66Session implements SessionInterface {
    /**
     * R66 Authentication
     */
    private AuthInterface r66Auth = null;

    /**
     * R66 DirInterface configuration and access
     */
    private DirInterface r66Dir = null;

    /**
     * Previous Command
     */
    private AbstractCommand previousCommand = null;

    /**
     * Current Command
     */
    private AbstractCommand currentCommand = null;

    /**
     * Associated Reply Code
     */
    private ReplyCode replyCode = null;

    /**
     * Real text for answer
     */
    private String answer = null;

    /**
     * Current Restart information
     */
    private Restart restart = null;

    private BusinessHandler businessHandler = null;
    /**
     * Is the control ready to accept command
     */
    private boolean isReady = false;
    /**
     * 
     */
    public R66Session(BusinessHandler businessHandler) {
        // TODO Auto-generated constructor stub
        this.businessHandler = businessHandler;
        isReady = false;
    }

    /**
	 * @return the businessHandler
	 */
	public BusinessHandler getBusinessHandler() {
		return businessHandler;
	}

	/* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#clean()
     */
    @Override
    public void clear() {
        // TODO Auto-generated method stub
        if (r66Dir != null) {
            r66Dir.clear();
            r66Dir = null;
        }
        if (r66Auth != null) {
            r66Auth.clear();
            r66Auth = null;
        }
        previousCommand = null;
        currentCommand = null;
        replyCode = null;
        answer = null;
        isReady = false;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getAuth()
     */
    @Override
    public AuthInterface getAuth() {
        return r66Auth;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getBlockSize()
     */
    @Override
    public int getBlockSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getDir()
     */
    @Override
    public DirInterface getDir() {
        return r66Dir;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getFileParameter()
     */
    @Override
    public FileParameterInterface getFileParameter() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getRestart()
     */
    @Override
    public Restart getRestart() {
        return restart;
    }
    /**
     * Set the new current command
     *
     * @param command
     */
    public void setNextCommand(CommandInterface command) {
        previousCommand = currentCommand;
        currentCommand = (AbstractCommand) command;
    }

    /**
     * @return the currentCommand
     */
    public AbstractCommand getCurrentCommand() {
        return currentCommand;
    }

    /**
     * @return the previousCommand
     */
    public AbstractCommand getPreviousCommand() {
        return previousCommand;
    }

    /**
     * Set the previous command as the new current command (used after a
     * incorrect sequence of commands or unknown command)
     *
     */
    public void setPreviousAsCurrentCommand() {
        currentCommand = previousCommand;
    }

    /**
     * @return the answer
     */
    public String getAnswer() {
        if (answer == null) {
            answer = replyCode.getMesg();
        }
        return answer;
    }

    /**
     * @param replyCode
     *            the replyCode to set
     * @param answer
     */
    public void setReplyCode(ReplyCode replyCode, String answer) {
        this.replyCode = replyCode;
        if (answer != null) {
            this.answer = ReplyCode.getFinalMsg(replyCode.getCode(), answer);
        } else {
            this.answer = replyCode.getMesg();
        }
    }

    /**
     * @param exception
     */
    public void setReplyCode(CommandAbstractException exception) {
        this.setReplyCode(exception.code, exception.message);
    }

    /**
     * Set Exit code after an error
     *
     * @param answer
     */
    public void setExitErrorCode(String answer) {
        this
                .setReplyCode(
                        ReplyCode.REPLY_421_SERVICE_NOT_AVAILABLE_CLOSING_CONTROL_CONNECTION,
                        answer);
    }

    /**
     * Set Exit normal code
     *
     * @param answer
     */
    public void setExitNormalCode(String answer) {
        this.setReplyCode(ReplyCode.REPLY_221_CLOSING_CONTROL_CONNECTION,
                answer);
    }

    /**
     * @return the replyCode
     */
    public ReplyCode getReplyCode() {
        return replyCode;
    }
    /**
     * @return True if the Control is ready to accept command
     */
    public boolean isReady() {
        return isReady;
    }

    /**
     * @param isReady
     *            the isReady to set
     */
    public void setReady(boolean isReady) {
        this.isReady = isReady;
    }
    @Override
    public String toString() {
        String mesg = "R66Session: ";
        if (currentCommand != null) {
            mesg += "CMD: " + currentCommand.getCommand() + " " +
                    currentCommand.getArg() + " ";
        }
        if (replyCode != null) {
            mesg += "Reply: " +
                    (answer != null? answer : replyCode
                            .getMesg()) + " ";
        }
        //FIXME XXX TODO 
        /*if (dataConn != null) {
            mesg += dataConn.toString();
        }*/
        if (r66Dir != null) {
            try {
                mesg += "PWD: " + r66Dir.getPwd();
            } catch (CommandAbstractException e) {
            }
        }
        return mesg + "\n";
    }
}
