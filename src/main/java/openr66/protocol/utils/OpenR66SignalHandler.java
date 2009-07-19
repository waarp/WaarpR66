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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import openr66.protocol.config.Configuration;

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
     * Set if the Handler is initialized
     */
    private static boolean initialized = false;

    /**
     * List all Connection to enable the close call on them
     */
    private static ConcurrentLinkedQueue<Connection> listConnection =
        new ConcurrentLinkedQueue<Connection>();

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
    public static void terminate(boolean immediate) {
        if (immediate) {
            shutdown = immediate;
        }
        terminate();
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
                    System.exit(1);
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
        Timer timer = null;
        timer = new Timer(true);
        final R66TimerTask timerTask = new R66TimerTask(R66TimerTask.TIMER_EXIT);
        timer.schedule(timerTask, Configuration.configuration.TIMEOUTCON * 2);
        if (shutdown) {
            ChannelUtils.exit();
            Connection con = listConnection.poll();
            while (con != null) {
                    try {
                            con.close();
                    } catch (SQLException e) {}
                    con = listConnection.poll();
            }
            // shouldn't be System.exit(2);
        } else {
            shutdown = true;
            ChannelUtils.exit();
            Connection con = listConnection.poll();
            while (con != null) {
                    try {
                            con.close();
                    } catch (SQLException e) {}
                    con = listConnection.poll();
            }
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
        // Not on WINDOWS
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
        System.exit(signal.getNumber());
    }

    /**
     * Add a Connection into the list
     *
     * @param conn
     */
    public static void addConnection(Connection conn) {
            listConnection.add(conn);
    }
    /**
     * Remove a Connection from the list
     * @param conn
     */
    public static void removeConnection(Connection conn) {
            listConnection.remove(conn);
    }
}
