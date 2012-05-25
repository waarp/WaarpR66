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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.R66Session;
import openr66.context.task.R66Runnable;

/**
 * Dummy Runnable Task that only logs
 * 
 * @author Frederic Bregier
 *
 */
public class AbstractExecJavaTask implements R66Runnable {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(AbstractExecJavaTask.class);
    
    protected int delay;
    protected String[] args = null;
    protected int status = -1;
    protected R66Session session;
    protected boolean waitForValidation;
    protected boolean useLocalExec;

    @Override
    public void run() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()+":");
        for (int i = 0; i < args.length; i++) {
            builder.append(' ');
            builder.append(args[i]);
        }
        logger.warn(builder.toString());
        this.status = 0;
    }

    @Override
    public void setArgs(R66Session session, boolean waitForValidation, 
            boolean useLocalExec, int delay, String []args) {
        this.session = session;
        this.waitForValidation = waitForValidation;
        this.useLocalExec = useLocalExec;
        this.delay = delay;
        this.args = args;
    }

    @Override
    public int getFinalStatus() {
        return status;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()+": [");
        builder.append(args[0]);
        builder.append("]");
        for (int i = 1; i < args.length ; i++) {
            builder.append(' ');
            builder.append(args[i]);
        }
        return builder.toString();
    }

}
