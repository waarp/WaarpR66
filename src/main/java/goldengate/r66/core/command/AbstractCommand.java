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
import goldengate.common.exception.InvalidArgumentException;
import goldengate.common.file.SessionInterface;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.r66.core.session.R66Session;

/**
 * @author Frederic Bregier
 *
 */
public abstract class AbstractCommand implements CommandInterface {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(AbstractCommand.class);

    /**
     * Code of Command
     */
    private R66CommandCode code;

    /**
     * String attached to the command
     */
    private String command;

    /**
     * Argument attached to this command
     */
    private String arg;

    /**
     * The R66 SessionInterface
     */
    private R66Session session;

    /**
     * Internal Object (whatever the used). This has to be clean by Business
     * Handler cleanSession.
     */
    private Object object;
    /**
     * Extra allowed nextCommand
     */
    private R66CommandCode extraNextCommand = null;
    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getArg()
     */
    @Override
    public String getArg() {
        return arg;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getArgs()
     */
    @Override
    public String[] getArgs() {
        return arg.split(" ");
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getCode()
     */
    @Override
    public R66CommandCode getCode() {
        return code;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getCommand()
     */
    @Override
    public String getCommand() {
        return command;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getObject()
     */
    @Override
    public Object getObject() {
        return object;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getSession()
     */
    @Override
    public R66Session getSession() {
        return session;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#getValue(java.lang.String)
     */
    @Override
    public int getValue(String argx) throws InvalidArgumentException {
        int i = 0;
        try {
            i = Integer.parseInt(argx);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentException("Not an integer");
        }
        return i;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#hasArg()
     */
    @Override
    public boolean hasArg() {
        return arg != null && arg.length() != 0;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#invalidCurrentCommand()
     */
    @Override
    public void invalidCurrentCommand() {
        session.getRestart().setSet(false);
        session.setPreviousAsCurrentCommand();
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#isNextCommandValid(goldengate.common.command.CommandInterface)
     */
    @Override
    public boolean isNextCommandValid(CommandInterface newCommandArg) {
        AbstractCommand newCommand = (AbstractCommand) newCommandArg;
        Class<? extends AbstractCommand> newClass = newCommand.getClass();
        // Special commands: QUIT ABORT STAT NOP
        if (R66CommandCode.isSpecialCommand(newCommand.getCode())) {
            logger.debug("VALID since {}", newCommand.command);
            return true;
        }
        if (extraNextCommand != null) {
            if (extraNextCommand.command == newClass) {
                logger.debug("VALID {} after {} since extra next command",
                        newCommand.command, command);
                return true;
            }
            if (code.nextValids != null &&
                    code.nextValids.length > 0) {
                for (Class<?> nextValid: code.nextValids) {
                    if (nextValid == newClass) {
                        logger.debug("VALID {} after {} since next command",
                                newCommand.command, command);
                        return true;
                    }
                }
            }
            logger.debug("NOT VALID {} after {}", newCommand.command,
                    command);
            return false;
        }
        if (code.nextValids == null ||
                code.nextValids.length == 0) {
            // Any command is allowed
            logger.debug("VALID {} after {} since all valid",
                    newCommand.command, command);
            return true;
        }
        for (Class<?> nextValid: code.nextValids) {
            if (nextValid == newClass) {
                logger.debug("VALID {} since next command {}",
                        newCommand.command, command);
                return true;
            }
        }
        logger.debug("DEFAULT NOT VALID {} after {}", newCommand.command,
                command);
        return false;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#setArgs(goldengate.common.file.SessionInterface, java.lang.String, java.lang.String, java.lang.Enum)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setArgs(SessionInterface session, String command, String arg,
            Enum code) {
        this.session = (R66Session) session;
        this.command = command;
        this.arg = arg;
        this.code = (R66CommandCode) code;
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#setExtraNextCommand(java.lang.Enum)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setExtraNextCommand(Enum extraNextCommand) {
        if (extraNextCommand != R66CommandCode.NOOP) {
            this.extraNextCommand = (R66CommandCode) extraNextCommand;
        } else {
            this.extraNextCommand = null;
        }
    }

    /* (non-Javadoc)
     * @see goldengate.common.command.CommandInterface#setObject(java.lang.Object)
     */
    @Override
    public void setObject(Object object) {
        this.object = object;
    }

}
