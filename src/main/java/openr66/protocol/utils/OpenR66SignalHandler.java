/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.protocol.utils;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.util.internal.SystemPropertyUtil;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Signal Handler to allow trapping signals.
 *
 * @author Frederic Bregier
 */
@SuppressWarnings("restriction")
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
     * List all Connection to enable the close call on them
     */
    private static ConcurrentHashMap<Long, Connection> listConnection = new ConcurrentHashMap<Long, Connection>();

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
     * @param immediate
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
    static private void printStackTrace(Thread thread, StackTraceElement[] stacks) {
        System.err.print(thread.toString() + " : ");
        for (int i = 0; i < stacks.length-1; i++) {
            System.err.print(stacks[i].toString()+" ");
        }
        System.err.println(stacks[stacks.length-1].toString());
    }

    /**
     * Finalize resources attached to handlers
     *
     * @author Frederic Bregier
     */
    private static class R66TimerTask extends TimerTask {
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
            super();
            this.type = type;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.TimerTask#run()
         */
        @Override
        public void run() {
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
                    logger.warn("Type unknown in TimerTask");
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
            // FIXME Force exit!
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
            Runtime.getRuntime().halt(0);
        } else {
            Timer timer = null;
            timer = new Timer(true);
            final R66TimerTask timerTask = new R66TimerTask(R66TimerTask.TIMER_EXIT);
            timer.schedule(timerTask, Configuration.configuration.TIMEOUTCON * 3);
            immediate = true;
            ChannelUtils.exit();
            System.exit(0);
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
                .toLowerCase().startsWith("windows"));
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

    /**
     * Add a Connection into the list
     *
     * @param conn
     */
    public static void addConnection(long id, Connection conn) {
        listConnection.put(Long.valueOf(id), conn);
    }

    /**
     * Remove a Connection from the list
     *
     * @param conn
     */
    public static void removeConnection(long id) {
        listConnection.remove(Long.valueOf(id));
    }
    /**
     *
     * @return the number of connection (so number of network channels)
     */
    public static int getNbConnection() {
        return listConnection.size()-1;
    }
    /**
     * Close all database connections
     */
    public static void closeAllConnection() {
        for (Connection con : listConnection.values()) {
            try {
                con.close();
            } catch (SQLException e) {
            }
        }
        listConnection.clear();
    }
}
