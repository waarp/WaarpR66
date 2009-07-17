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

import openr66.filesystem.R66Session;
import openr66.protocol.exception.OpenR66RunnerErrorException;

/**
 * @author Frederic Bregier
 *
 */
public class TaskFactory {

    private static TaskType getEnumFromName(String name) {
        return TaskType.valueOf(name);
    }

    public static AbstractTask getTaskFromId(String name, String argRule, R66Session session)
    throws OpenR66RunnerErrorException {
        TaskType type = getEnumFromName(name);
        switch (type) {
            case TEST:
                return new TestTask(argRule, session.getRequest().getFileInformation(), session);
            case MOVE:
                return new MoveTask(argRule, session.getRequest().getFileInformation(), session);
            case MOVERENAME:
                return new MoveRenameTask(argRule, session.getRequest().getFileInformation(), session);
            case COPY:
                return new CopyTask(argRule, session.getRequest().getFileInformation(), session);
            case COPYRENAME:
                return new CopyRenameTask(argRule, session.getRequest().getFileInformation(), session);
            default:
                throw new OpenR66RunnerErrorException(
                        "Unvalid Task: " + type.name);
        }
    }
}
