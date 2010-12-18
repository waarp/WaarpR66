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
package openr66.protocol.networkhandler;

import goldengate.common.cpu.CpuManagement;
import goldengate.common.cpu.CpuManagementInterface;
import goldengate.common.cpu.CpuManagementNoInfo;
import goldengate.common.cpu.CpuManagementSysmon;
import goldengate.common.database.DbAdmin;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.Random;

import openr66.protocol.configuration.Configuration;

/**
 * Constraint Limit (CPU and connection - network and local -) handler, only for server side (requested or requester).
 * @author Frederic Bregier
 *
 */
public class ConstraintLimitHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ConstraintLimitHandler.class);
    private static Random random = new Random();
    private static CpuManagementInterface cpuManagement;
    private static double cpuLimit = 0.8;
    private static int channelLimit = 1000;
    private boolean isServer = false;
    private double lastLA = 0.0;
    private long lastTime;
    
    public ConstraintLimitHandler(boolean useCpuLimit, boolean useJdKCpuLimit, double cpulimit, int channellimit) {
        if (useCpuLimit) {
            if (useJdKCpuLimit) {
                try {
                    cpuManagement = new CpuManagement();
                } catch (IllegalArgumentException e) {
                    cpuManagement = new CpuManagementNoInfo();
                }
            } else {
                cpuManagement = new CpuManagementSysmon();
            }
        } else {
            cpuManagement = new CpuManagementNoInfo();
        }
        cpuLimit = cpulimit;
        channelLimit = channellimit;
        lastTime = System.currentTimeMillis();
    }
    /**
     * To explicitely set this handler as server mode
     * @param isServer
     */
    public void setServer(boolean isServer) {
        this.isServer = isServer;
    }
    /**
     * 
     * @return True if one of the limit is exceeded. Always False if not a server mode
     */
    public boolean checkConstraints() {
        if (! isServer)
            return false;
        if (cpuLimit < 1 && cpuLimit > 0) {
            long newTime = System.currentTimeMillis();
            // first check if last test was done too shortly
            if ((newTime - lastTime) < (Configuration.WAITFORNETOP/2)) {
                // If last test was wrong, then redo the test
                if (lastLA <= cpuLimit) {
                    // last test was OK, so Continue
                    return false;
                }
            }
            lastTime = newTime;
            lastLA = cpuManagement.getLoadAverage();
            if (lastLA > cpuLimit) {
                logger.debug("LA: "+lastLA+" > "+cpuLimit);
                return true;
            }
        }
        if (channelLimit > 0) {
            int nb = DbAdmin.getNbConnection();
            if (channelLimit <= nb) {
                logger.debug("NW:"+nb+" > "+channelLimit);
                return true;
            }
            nb = Configuration.configuration.getLocalTransaction().getNumberLocalChannel();
            if (channelLimit <= nb) {
                logger.debug("NL:"+nb+" > "+channelLimit);
                return true;
            }
        }
        return false;
    }
    /**
     * Same as checkConstraints except that the thread will sleep some time proportionally to
     * the current Load (if CPU related)
     * @param step the current step in retry
     * @return True if one of the limit is exceeded. Always False if not a server mode
     */
    public boolean checkConstraintsSleep(int step) {
        if (! isServer)
            return false;
        long delay = Configuration.WAITFORNETOP/2;
        if (cpuLimit < 1 && cpuLimit > 0) {
            long newTime = System.currentTimeMillis();
            // first check if last test was done too shortly
            if ((newTime - lastTime) < delay) {
                // If last test was wrong, then wait a bit then redo the test
                if (lastLA > cpuLimit) {
                    double sleep = lastLA * delay * (step+1) * random.nextFloat();
                    long shorttime = (long) sleep;
                    try {
                        Thread.sleep(shorttime);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // last test was OK, so Continue
                    return false;
                }
            }
        }
        if (checkConstraints()) {
            delay = getSleepTime()*(step+1);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 
     * @return a time below TIMEOUTCON with a random
     */
    public static long getSleepTime() {
        return (long) (Configuration.configuration.TIMEOUTCON*random.nextFloat())+100;
    }
    /**
     * @return the cpuLimit
     */
    public static double getCpuLimit() {
        return cpuLimit;
    }

    /**
     * @param cpuLimit the cpuLimit to set
     */
    public static void setCpuLimit(double cpuLimit) {
        ConstraintLimitHandler.cpuLimit = cpuLimit;
    }

    /**
     * @return the channelLimit
     */
    public static int getChannelLimit() {
        return channelLimit;
    }

    /**
     * @param channelLimit the channelLimit to set
     */
    public static void setChannelLimit(int channelLimit) {
        ConstraintLimitHandler.channelLimit = channelLimit;
    }
    
}
