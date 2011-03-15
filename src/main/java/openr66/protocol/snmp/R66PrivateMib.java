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
package openr66.protocol.snmp;

import org.snmp4j.agent.MOScope;
import org.snmp4j.smi.OID;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.snmp.GgMOScalar;
import goldengate.snmp.GgPrivateMib;

/**
 * GoldenGate OpenR66 Private MIB implementation
 * 
 * @author Frederic Bregier
 *
 */
public class R66PrivateMib extends GgPrivateMib {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66PrivateMib.class);

    /**
     * @param sysdesc
     * @param port
     * @param smiPrivateCodeFinal
     * @param typeGoldenGateObject
     * @param scontactName
     * @param stextualName
     * @param saddress
     * @param iservice
     */
    public R66PrivateMib(String sysdesc, int port, int smiPrivateCodeFinal,
            int typeGoldenGateObject, String scontactName, String stextualName,
            String saddress, int iservice) {
        super(sysdesc, port, smiPrivateCodeFinal, typeGoldenGateObject,
                scontactName, stextualName, saddress, iservice);
    }

    /* (non-Javadoc)
     * @see goldengate.snmp.GgInterfaceMib#updateServices(goldengate.snmp.GgMOScalar)
     */
    @Override
    public void updateServices(GgMOScalar scalar) {
        // 3 groups to check
        OID oid = scalar.getOid();
        if (oid.startsWith(rootOIDGoldenGateGeneral)) {
            // UpTime
            if (oid.equals(rootOIDGoldenGateGeneralUptime)) {
                scalarUptime.setValue(upTime.get());
                return;
            }
            agent.monitor.generalValuesUpdate();
        } else if (oid.startsWith(rootOIDGoldenGateDetailed)) {
            agent.monitor.detailedValuesUpdate();
        } else if (oid.startsWith(rootOIDGoldenGateError)) {
            agent.monitor.errorValuesUpdate();
        }
    }

    /* (non-Javadoc)
     * @see goldengate.snmp.GgInterfaceMib#updateServices(org.snmp4j.agent.MOScope)
     */
    @Override
    public void updateServices(MOScope range) {
        // UpTime first
        OID low = range.getLowerBound();
        
        boolean okGeneral = true;
        boolean okDetailed = true;
        boolean okError = true;
        if (low != null) {
            logger.debug("low: {}:{} "+rootOIDGoldenGateGeneral+":"+
                    rootOIDGoldenGateDetailed+":"+rootOIDGoldenGateError,
                    low,range.isLowerIncluded());
            if (low.size() <= rootOIDGoldenGate.size() && low.startsWith(rootOIDGoldenGate)) {
                // test for global requests
                okGeneral = okDetailed = okError = true;
            } else {
                // Test for sub requests
                okGeneral &= low.startsWith(rootOIDGoldenGateGeneral);
                okDetailed &= low.startsWith(rootOIDGoldenGateDetailed);
                okError &= low.startsWith(rootOIDGoldenGateError);
            }
        }
        logger.debug("General:"+okGeneral+" Detailed:"+okDetailed+" Error:"+okError);
        if (okGeneral) {
            // UpTime
            if (rootOIDGoldenGateGeneralUptime.compareTo(low) >= 0)
                scalarUptime.setValue(upTime.get());
            agent.monitor.generalValuesUpdate();
        }
        if (okDetailed) {
            agent.monitor.detailedValuesUpdate();
        }
        if (okError) {
            agent.monitor.errorValuesUpdate();
        }
    }

}
