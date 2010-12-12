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

import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;

/**
 * This enum class references all available tasks.
 *
 * If a new task type is to be created, this is the place where it should be
 * referenced.
 *
 * @author Frederic Bregier
 *
 */
public enum TaskType {
    LOG, MOVE, MOVERENAME, COPY, COPYRENAME, EXEC, EXECMOVE, LINKRENAME, TRANSFER,
    VALIDFILEPATH, DELETE, TAR, ZIP;

    public int type;

    public String name;

    private TaskType() {
        type = ordinal();
        name = name();
    }

    /**
     *
     * @param type
     * @param argRule
     * @param delay
     * @param session
     * @return the corresponding AbstractTask
     * @throws OpenR66RunnerErrorException
     */
    public static AbstractTask getTaskFromId(TaskType type, String argRule,
            int delay, R66Session session)
            throws OpenR66RunnerErrorException {
        switch (type) {
            case LOG:
                return new LogTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case MOVE:
                return new MoveTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case MOVERENAME:
                return new MoveRenameTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case COPY:
                return new CopyTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case COPYRENAME:
                return new CopyRenameTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case EXEC:
                return new ExecTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case EXECMOVE:
                return new ExecMoveTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case LINKRENAME:
                return new LinkRenameTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case TRANSFER:
                return new TransferTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case VALIDFILEPATH:
                return new ValidFilePathTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case DELETE:
                return new DeleteTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case TAR:
                return new TarTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            case ZIP:
                return new ZipTask(argRule, delay, session.getRunner()
                        .getFileInformation(), session);
            default:
                throw new OpenR66RunnerErrorException("Unvalid Task: " +
                        type.name);
        }
    }

    /**
     *
     * @param name
     * @param argRule
     * @param delay
     * @param session
     * @return the corresponding AbstractTask
     * @throws OpenR66RunnerErrorException
     */
    public static AbstractTask getTaskFromId(String name, String argRule,
            int delay, R66Session session) throws OpenR66RunnerErrorException {
        TaskType type;
        try {
            type = valueOf(name);
        } catch (NullPointerException e) {
            System.err.println("name: " + name);
            throw new OpenR66RunnerErrorException("Unvalid Task: " + name);
        } catch (IllegalArgumentException e) {
            throw new OpenR66RunnerErrorException("Unvalid Task: " + name);
        }
        return getTaskFromId(type, argRule, delay, session);
    }
}
