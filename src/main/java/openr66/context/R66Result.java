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
package openr66.context;

import openr66.context.filesystem.R66File;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.exception.OpenR66Exception;

/**
 * This class is the result for every operations in OpenR66.
 *
 * @author Frederic Bregier
 *
 */
public class R66Result {
    /**
     * The exception associated in case of error (if any exception)
     */
    public OpenR66Exception exception = null;
    /**
     * The file if any
     */
    public R66File file = null;
    /**
     * The runner if any
     */
    public DbTaskRunner runner = null;
    /**
     * Does this result already have been transfered to the remote server
     */
    public boolean isAnswered = false;
    /**
     * The code (error or not)
     */
    public R66ErrorCode code;
    /**
     * Any other object for special operations (test or shutdown for instance)
     */
    public Object other = null;

    /**
     * @param exception
     * @param session
     * @param isAnswered
     */
    public R66Result(OpenR66Exception exception, R66Session session,
            boolean isAnswered, R66ErrorCode code) {
        this.exception = exception;
        if (session != null) {
            file = session.getFile();
            runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
        this.code = code;
    }

    /**
     * @param session
     * @param isAnswered
     */
    public R66Result(R66Session session, boolean isAnswered, R66ErrorCode code) {
        if (session != null) {
            file = session.getFile();
            runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
        this.code = code;
    }

    @Override
    public String toString() {
        return (exception != null? "Exception: " + exception.toString() : "") +
                (file != null? file.toString() : " no file") +
                (runner != null? runner.toString() : " no runner") +
                " isAnswered: " + isAnswered + " Code: " + code.mesg;
    }
}
