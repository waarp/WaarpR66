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
package openr66.client;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.database.DbConstant;
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
    static protected volatile GgInternalLogger logger;

    protected final R66Future future;

    protected final String filename;

    protected final String rulename;

    protected final String fileinfo;

    protected final boolean isMD5;

    protected final String remoteHost;

    protected final int blocksize;
    
    protected final long id;


    /**
     * @param clasz Class of Client Transfer
     * @param future
     * @param filename
     * @param rulename
     * @param fileinfo
     * @param isMD5
     * @param remoteHost
     * @param blocksize
     * @param id
     */
    public AbstractTransfer(Class<?> clasz, R66Future future, String filename,
            String rulename, String fileinfo,
            boolean isMD5, String remoteHost, int blocksize, long id) {
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
        this.id = id;
    }

    static protected String rhost = null;
    static protected String localFilename = null;
    static protected String rule = null;
    static protected String fileInfo = null;
    static protected boolean ismd5 = false;
    static protected int block = 0x10000; // 64K as default
    static protected boolean nolog = false;
    static protected long idt = DbConstant.ILLEGALVALUE;
    
    /**
     * Parse the parameter and set current values
     * @param args
     * @param submitOnly True if the client is only a submitter (through database)
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args, boolean submitOnly) {
        if (args.length < 4) {
            logger
                    .error("Needs at least 4 arguments:\n" +
                    		"  the XML client configuration file,\n" +
                    		"  '-to' the remoteHost Id,\n" +
                                "  '-file' the file to transfer,\n" +
                                "  '-rule' the rule\n"+
                                "Other options:\n" +
                                "   '-id' \"Id of a previous transfer\",\n"+
                                "  '-info' \"information to send\",\n" +
                                "  '-md5' to force MD5 by packet control,\n" +
                                "  '-block' size of packet > 1K (prefered is 64K),\n" +
                                "  '-nolog' to not log locally this action");
            return false;
        }
        if (submitOnly) {
            if (! FileBasedConfiguration
                    .setSubmitClientConfigurationFromXml(args[0])) {
                logger
                        .error("Needs a correct configuration file as first argument");
                return false;
            }
        } else if (! FileBasedConfiguration
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
            } else if (args[i].equalsIgnoreCase("-id")) {
                i++;
                idt = Long.parseLong(args[i]);
            }
        }
        if (fileInfo == null) {
            fileInfo = "noinfo";
        }
        if (rhost != null && rule != null && localFilename != null) {
            return true;
        }
        logger.error("All params are not set! Need at least -to -rule and -file params");
        return false;
    }
}
