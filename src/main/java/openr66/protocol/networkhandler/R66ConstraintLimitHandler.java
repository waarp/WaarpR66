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

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.handler.traffic.GlobalTrafficShapingHandler;

import goldengate.common.cpu.GgConstraintLimitHandler;

/**
 * R66 Constraint Limit Handler
 * 
 * Constraint Limit (CPU and connection - network and local -) handler, 
 * only for server side (requested or requester).
 * 
 * @author Frederic Bregier
 *
 */
public class R66ConstraintLimitHandler extends GgConstraintLimitHandler {

    /**
     * @param useJdKCpuLimit True to use JDK Cpu native or False for JavaSysMon
     * @param lowcpuLimit for proactive cpu limitation (throttling bandwidth) (0<= x < 1 & highcpulimit) 
     * @param highcpuLimit for proactive cpu limitation (throttling bandwidth) (0<= x <= 1) 0 meaning no throttle activated
     * @param percentageDecrease for proactive cpu limitation, throttling bandwidth reduction (0 < x < 1) as 0.25 for 25% of reduction
     * @param handler the GlobalTrafficShapingHandler associated (null to have no proactive cpu limitation)
     * @param delay the delay between 2 tests for proactive cpu limitation
     * @param limitLowBandwidth the minimal bandwidth (read or write) to apply when decreasing bandwidth (low limit = 4096)
     */
    public R66ConstraintLimitHandler(boolean useJdKCpuLimit,
            double lowcpuLimit, double highcpuLimit, double percentageDecrease,
            GlobalTrafficShapingHandler handler, long delay,
            long limitLowBandwidth) {
        super(Configuration.WAITFORNETOP, Configuration.configuration.TIMEOUTCON,
                useJdKCpuLimit,
                lowcpuLimit, highcpuLimit,
                percentageDecrease, handler, delay, limitLowBandwidth);
    }

    /**
     * @param useCpuLimit True to enable cpuLimit on connection check
     * @param useJdKCpuLimit True to use JDK Cpu native or False for JavaSysMon
     * @param cpulimit high cpu limit (0<= x < 1) to refuse new connections
     * @param channellimit number of connection limit (0<= x)
     */
    public R66ConstraintLimitHandler(boolean useCpuLimit,
            boolean useJdKCpuLimit, double cpulimit, int channellimit) {
        super(useCpuLimit, useJdKCpuLimit, cpulimit, channellimit);
    }

    /**
     * @param useCpuLimit True to enable cpuLimit on connection check
     * @param useJdKCpuLimit True to use JDK Cpu native or False for JavaSysMon
     * @param cpulimit high cpu limit (0<= x < 1) to refuse new connections
     * @param channellimit number of connection limit (0<= x)
     * @param lowcpuLimit for proactive cpu limitation (throttling bandwidth) (0<= x < 1 & highcpulimit) 
     * @param highcpuLimit for proactive cpu limitation (throttling bandwidth) (0<= x <= 1) 0 meaning no throttle activated
     * @param percentageDecrease for proactive cpu limitation, throttling bandwidth reduction (0 < x < 1) as 0.25 for 25% of reduction
     * @param handler the GlobalTrafficShapingHandler associated (null to have no proactive cpu limitation)
     * @param delay the delay between 2 tests for proactive cpu limitation
     * @param limitLowBandwidth the minimal bandwidth (read or write) to apply when decreasing bandwidth (low limit = 4096)
     */
    public R66ConstraintLimitHandler(
            boolean useCpuLimit, boolean useJdKCpuLimit, double cpulimit,
            int channellimit, double lowcpuLimit, double highcpuLimit,
            double percentageDecrease, GlobalTrafficShapingHandler handler,
            long delay, long limitLowBandwidth) {
        super(Configuration.WAITFORNETOP, Configuration.configuration.TIMEOUTCON,
                useCpuLimit, useJdKCpuLimit,
                cpulimit, channellimit, lowcpuLimit, highcpuLimit,
                percentageDecrease, handler, delay, limitLowBandwidth);
    }

    /* (non-Javadoc)
     * @see goldengate.common.cpu.GgConstraintLimitHandler#getNumberLocalChannel()
     */
    @Override
    protected int getNumberLocalChannel() {
        return Configuration.configuration.getLocalTransaction().getNumberLocalChannel();
    }

    /* (non-Javadoc)
     * @see goldengate.common.cpu.GgConstraintLimitHandler#getReadLimit()
     */
    @Override
    protected long getReadLimit() {
        return Configuration.configuration.serverGlobalReadLimit;
    }

    /* (non-Javadoc)
     * @see goldengate.common.cpu.GgConstraintLimitHandler#getWriteLimit()
     */
    @Override
    protected long getWriteLimit() {
        return Configuration.configuration.serverGlobalWriteLimit;
    }

}
