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
    public ErrorCode code;
    /**
     * Any other object for special operations (test or shutdown for instance)
     */
    public Object other = null;

    /**
     * @param exception
     * @param session
     * @param isAnswered
     * @param code
     * @param runner
     */
    public R66Result(OpenR66Exception exception, R66Session session,
            boolean isAnswered, ErrorCode code, DbTaskRunner runner) {
        this.exception = exception;
        this.runner = runner;
        if (session != null) {
            file = session.getFile();
            this.runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
        this.code = code;
    }

    /**
     * @param session
     * @param isAnswered
     * @param code
     * @param runner
     */
    public R66Result(R66Session session, boolean isAnswered, ErrorCode code,
            DbTaskRunner runner) {
        this.runner = runner;
        if (session != null) {
            file = session.getFile();
            this.runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
        this.code = code;
    }

    @Override
    public String toString() {
        return (exception != null? "Exception: " + exception.toString() : "") +
                (file != null? file.toString() : " no file") + "\n    "+
                (runner != null? runner.toShortString() : " no runner") +
                " isAnswered: " + isAnswered + " Code: " + code.mesg;
    }
    /**
     * 
     * @return the associated message with this Result
     */
    public String getMessage() {
    	if (exception != null) {
    		return exception.getMessage();
    	} else {
    		return code.mesg;
    	}
    }
}
