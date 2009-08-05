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

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.protocol.utils.R66Future;

/**
 * Abstract class for Transfer operation
 *
 * @author Frederic Bregier
 *
 */
public abstract class AbstractTransfer implements Runnable {
    /**
     * Internal Logger
     */
    static protected GgInternalLogger logger;

    protected final R66Future future;

    protected final String filename;

    protected final String rulename;

    protected final String fileinfo;

    protected final boolean isMD5;

    protected final String remoteHost;

    protected final int blocksize;


    /**
     * @param clasz Class of Client Transfer
     * @param future
     * @param filename
     * @param rulename
     * @param fileinfo
     * @param isMD5
     * @param remoteHost
     * @param blocksize
     */
    public AbstractTransfer(Class<?> clasz, R66Future future, String filename,
            String rulename, String fileinfo,
            boolean isMD5, String remoteHost, int blocksize) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(clasz);
        }
        this.future = future;
        this.filename = filename;
        this.rulename = rulename;
        this.fileinfo = fileinfo;
        this.isMD5 = isMD5;
        this.remoteHost = remoteHost;
        this.blocksize = blocksize;
    }

    static protected String rhost = null;
    static protected String localFilename = null;
    static protected String rule = null;
    static protected String fileInfo = null;
    static protected boolean ismd5 = false;
    static protected int block = 0x10000; // 64K as default
    static protected boolean nolog = false;

    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 4) {
            logger
                    .error("Needs at least the configuration file, the remoteHost Id, " +
                                "the file to transfer, the rule");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-to")) {
                i++;
                rhost = args[i];
            } else if (args[i].equalsIgnoreCase("-file")) {
                i++;
                localFilename = args[i];
            } else if (args[i].equalsIgnoreCase("-rule")) {
                i++;
                rule = args[i];
            } else if (args[i].equalsIgnoreCase("-info")) {
                i++;
                fileInfo = args[i];
            } else if (args[i].equalsIgnoreCase("-md5")) {
                ismd5 = true;
            } else if (args[i].equalsIgnoreCase("-block")) {
                i++;
                block = Integer.parseInt(args[i]);
                if (block < 100) {
                    logger.error("Block size is too small: "+block);
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-nolog")) {
                nolog = true;
                i++;
            }
        }
        if (rhost != null && rule != null && localFilename != null) {
            return true;
        }
        logger.error("All params are not set! Need at least -to -rule and -file params");
        return false;
    }
}
