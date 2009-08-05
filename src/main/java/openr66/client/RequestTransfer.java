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
package openr66.client;

import java.io.File;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;

/**
 * Class to request information or request cancellation or restart
 *
 * @author Frederic Bregier
 *
 */
public class RequestTransfer {
    /**
     * Internal Logger
     */
    static GgInternalLogger logger;

    static long specialId;
    static String requested = null;
    static boolean cancel = false;
    static boolean stop = false;
    static boolean restart = false;

    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 3) {
            logger
                    .error("Needs at least the configuration file, the transfer id and " +
                    		"the requested hostId");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-id")) {
                i++;
                specialId = Long.parseLong(args[i]);
            } else if (args[i].equalsIgnoreCase("-to")) {
                i++;
                requested = args[i];
            } else if (args[i].equalsIgnoreCase("-cancel")) {
                cancel = true;
            } else if (args[i].equalsIgnoreCase("-stop")) {
                stop = true;
            } else if (args[i].equalsIgnoreCase("-restart")) {
                restart = true;
            }
        }
        if ((cancel && restart) || (cancel && stop)) {
            logger.error("Cannot cancel and restart/stop at the same time");
            return false;
        }
        if (specialId == DbConstant.ILLEGALVALUE || requested == null) {
            logger.error("TransferId and Requested HostId must be set");
            return false;
        }

        return true;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(RequestTransfer.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
            System.exit(1);
        }
        try {
            DbTaskRunner runner = null;
            try {
                runner = new DbTaskRunner(DbConstant.admin.session,null,null,
                        specialId,Configuration.configuration.HOST_ID,requested);
            } catch (OpenR66DatabaseException e) {
                logger.error("Cannot find the transfer", e);
                return;
            }
            if (cancel || stop || restart) {
                if (cancel) {
                    // Cancel the task and delete any file if in retrieve
                    if (runner.isFinished()) {
                        if (runner.isRetrieve()) {
                            String filename = Configuration.configuration.baseDirectory+
                                runner.getFilename();
                            File file = new File(filename);
                            if (file.exists()) {
                                file.delete();
                            }
                        }
                        try {
                            runner.delete();
                        } catch (OpenR66DatabaseException e) {
                            logger.error("Cannot delete the transfer", e);
                            return;
                        }
                    } else {
                        // First stop it
                        // Send a request
                    }
                } else if (stop) {
                    // Just stop the task
                    // Send a request
                } else {
                    // Restart if already stopped

                }
            } else {
                // Only request
                logger.warn("Transfer: "+runner.toString());
            }
        } finally {
            if (DbConstant.admin != null) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
        }
    }

}
