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
package openr66.protocol.utils;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.jboss.netty.util.internal.SystemPropertyUtil;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Signal Handler to allow trapping signals.
 *
 * @author Frederic Bregier
 */
public class OpenR66SignalHandler implements SignalHandler {
    /**
     * Set if the program is in shutdown
     */
    private static volatile boolean shutdown = false;

    /**
     * Set if the program is in shutdown
     */
    private static volatile boolean immediate = false;

    /**
     * Set if the Handler is initialized
     */
    private static boolean initialized = false;

    /**
     * Previous Handler
     */
    private SignalHandler oldHandler = null;

    /**
     * Says if the Process is currently in shutdown
     *
     * @return True if already in shutdown
     */
    public static boolean isInShutdown() {
        return shutdown;
    }

    /**
     * This function is the top function to be called when the process is to be
     * shutdown.
     *
     * @param immediateSet
     */
    public static void terminate(boolean immediateSet) {
        if (immediateSet) {
            immediate = immediateSet;
        }
        terminate();
    }

    /**
     * Print stack trace
     * @param thread
     * @param stacks
     */
    static public void printStackTrace(Thread thread, StackTraceElement[] stacks) {
        System.err.print(thread.toString() + " : ");
        for (int i = 0; i < stacks.length-1; i++) {
            System.err.print(stacks[i].toString()+" ");
        }
        if (stacks.length >= 1)
            System.err.println(stacks[stacks.length-1].toString());
    }

    /**
     * Finalize resources attached to handlers
     *
     * @author Frederic Bregier
     */
    public static class R66TimerTask implements TimerTask {
        /**
         * Internal Logger
         */
        private static final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(R66TimerTask.class);

        /**
         * EXIT type (System.exit(1))
         */
        public static final int TIMER_EXIT = 1;

        /**
         * Type of execution in run() method
         */
        private final int type;

        /**
         * Constructor from type
         *
         * @param type
         */
        public R66TimerTask(int type) {
            this.type = type;
        }

        /* (non-Javadoc)
         * @see org.jboss.netty.util.TimerTask#run(org.jboss.netty.util.Timeout)
         */
        @Override
        public void run(Timeout timeout) throws Exception {
            switch (type) {
                case TIMER_EXIT:
                    logger.error("System will force EXIT");
                    Map<Thread, StackTraceElement[]> map = Thread
                            .getAllStackTraces();
                    for (Thread thread: map.keySet()) {
                        printStackTrace(thread, map.get(thread));
                    }
                    System.exit(0);
                    break;
                default:
                    logger.error("Type unknown in TimerTask");
            }
        }
    }

    /**
     * Function to terminate IoSession and Connection.
     */
    private static void terminate() {
        shutdown = true;
        if (immediate) {
            ChannelUtils.exit();
            // Force exit!
            try {
                Thread.sleep(Configuration.configuration.TIMEOUTCON);
            } catch (InterruptedException e) {
            }
            Map<Thread, StackTraceElement[]> map = Thread
                .getAllStackTraces();
            for (Thread thread: map.keySet()) {
                printStackTrace(thread, map.get(thread));
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            System.err.println("Halt System");
            Runtime.getRuntime().halt(0);
        } else {
            R66TimerTask timerTask = new R66TimerTask(R66TimerTask.TIMER_EXIT);
            Configuration.configuration.getTimerClose().newTimeout(timerTask, Configuration.configuration.TIMEOUTCON * 2, TimeUnit.MILLISECONDS);
            immediate = true;
            ChannelUtils.exit();
            System.err.println("Exit System");
            //System.exit(0);
        }
    }

    /**
     * Function to initialized the SignalHandler
     */
    public static void initSignalHandler() {
        if (initialized) {
            return;
        }
        Signal diagSignal = new Signal("TERM");
        OpenR66SignalHandler diagHandler = new OpenR66SignalHandler();
        diagHandler.oldHandler = Signal.handle(diagSignal, diagHandler);
        // Not on WINDOWS ?
        Configuration.ISUNIX = (!System.getProperty("os.name")
                .toLowerCase().startsWith("win"));
        if (Configuration.ISUNIX) {
            String vendor = SystemPropertyUtil.get("java.vm.vendor");
            vendor = vendor.toLowerCase();
            if (vendor.indexOf("ibm") >= 0) {
                diagSignal = new Signal("USR1");
                diagHandler = new OpenR66SignalHandler();
                diagHandler.oldHandler = Signal.handle(diagSignal, diagHandler);
            }
        }
        initialized = true;
    }

    /**
     * Handle signal
     *
     * @param signal
     */
    public void handle(Signal signal) {
        try {
            terminate();
            // Chain back to previous handler, if one exists
            if (oldHandler != SIG_DFL && oldHandler != SIG_IGN) {
                oldHandler.handle(signal);
            }
        } catch (final Exception e) {
        }
        //ChannelUtils.stopLogger();
        System.err.println("Signal: "+signal.getNumber());
        System.exit(0);
    }
}
