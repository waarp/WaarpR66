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
package openr66.context;

import openr66.context.filesystem.R66File;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.exception.OpenR66Exception;

/**
 * @author Frederic Bregier
 *
 */
public class R66Result {
    public OpenR66Exception exception = null;
    public R66File file = null;
    public DbTaskRunner runner = null;
    public boolean isAnswered = false;
    public Object other = null;
    /**
     * @param exception
     * @param session
     * @param isAnswered
     */
    public R66Result(OpenR66Exception exception, R66Session session, boolean isAnswered) {
        this.exception = exception;
        if (session != null) {
            this.file = session.getFile();
            this.runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
    }
    /**
     * @param session
     * @param isAnswered
     */
    public R66Result(R66Session session, boolean isAnswered) {
        if (session != null) {
            this.file = session.getFile();
            this.runner = session.getRunner();
        }
        this.isAnswered = isAnswered;
    }
    @Override
    public String toString() {
        return (exception != null ? "Exception: "+exception.toString() : "") +
            (file != null ? file.toString() : " no file") +
            (runner != null ? runner.toString() : " no runner")+
            " isAnswered: "+isAnswered;
    }
}
