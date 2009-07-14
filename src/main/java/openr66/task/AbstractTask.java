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
import openr66.protocol.utils.R66Future;

/**
 * @author Frederic Bregier
 *
 */
public abstract class AbstractTask implements Runnable {
    /**
     * Type of operation
     */
    final TaskFactory.TaskType type;
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
    AbstractTask(TaskFactory.TaskType type, String argRule, String argTransfer, R66Session session) {
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

}
