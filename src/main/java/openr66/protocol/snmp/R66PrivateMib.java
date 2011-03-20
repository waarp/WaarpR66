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

import openr66.protocol.configuration.Configuration;
import openr66.protocol.utils.Version;

import org.snmp4j.agent.DuplicateRegistrationException;
import org.snmp4j.agent.MOScope;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.snmp.r66.GgPrivateMib;
import goldengate.snmp.utils.GgMORow;
import goldengate.snmp.utils.GgMOScalar;
import goldengate.snmp.utils.GgUptime;
import goldengate.snmp.utils.MemoryGauge32;
import goldengate.snmp.utils.MemoryGauge32.MemoryType;

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
     * @see goldengate.snmp.GgPrivateMib#agentRegisterGoldenGateMib()
     */
    @Override
    protected void agentRegisterGoldenGateMib()
            throws DuplicateRegistrationException {
        logger.debug("registerGGMib");
        // register Static info
        rowInfo = new GgMORow(this, rootOIDGoldenGateInfo, goldenGateDefinition, 
                MibLevel.staticInfo.ordinal());
        rowInfo.setValue(goldenGateDefinitionIndex.applName.ordinal(), 
                "GoldenGate OpenR66");
        rowInfo.setValue(goldenGateDefinitionIndex.applServerName.ordinal(), 
                Configuration.configuration.HOST_ID);
        rowInfo.setValue(goldenGateDefinitionIndex.applVersion.ordinal(), 
                Version.ID);
        rowInfo.setValue(goldenGateDefinitionIndex.applDescription.ordinal(), 
                "GoldenGate OpenR66: File Transfer Monitor");
        rowInfo.setValue(goldenGateDefinitionIndex.applURL.ordinal(), 
                "http://openr66.free.fr");
        rowInfo.setValue(goldenGateDefinitionIndex.applApplicationProtocol.ordinal(), 
                applicationProtocol);
        
        rowInfo.registerMOs(agent.getServer(), null);
        // register General info
        rowGlobal = new GgMORow(this, rootOIDGoldenGateGlobal, goldenGateGlobalValues, 
                MibLevel.globalInfo.ordinal());
        GgMOScalar memoryScalar = rowGlobal.row[goldenGateGlobalValuesIndex.memoryTotal.ordinal()];
        memoryScalar.setValue(new MemoryGauge32(MemoryType.TotalMemory));
        memoryScalar = rowGlobal.row[goldenGateGlobalValuesIndex.memoryFree.ordinal()];
        memoryScalar.setValue(new MemoryGauge32(MemoryType.FreeMemory));
        memoryScalar = rowGlobal.row[goldenGateGlobalValuesIndex.memoryUsed.ordinal()];
        memoryScalar.setValue(new MemoryGauge32(MemoryType.UsedMemory));
        rowGlobal.registerMOs(agent.getServer(), null);
        // setup UpTime to SysUpTime and change status
        scalarUptime = rowGlobal.row[goldenGateGlobalValuesIndex.applUptime.ordinal()];
        scalarUptime.setValue(new GgUptime(upTime));
        changeStatus(OperStatus.restarting);
        changeStatus(OperStatus.up);
        // register Detailed info
        rowDetailed = new GgMORow(this, rootOIDGoldenGateDetailed, goldenGateDetailedValues, 
                MibLevel.detailedInfo.ordinal());
        rowDetailed.registerMOs(agent.getServer(), null);
        // register Error info
        rowError = new GgMORow(this, rootOIDGoldenGateError, goldenGateErrorValues, 
                MibLevel.errorInfo.ordinal());
        rowError.registerMOs(agent.getServer(), null);
    }

    /* (non-Javadoc)
     * @see goldengate.snmp.GgInterfaceMib#updateServices(goldengate.snmp.GgMOScalar)
     */
    @Override
    public void updateServices(GgMOScalar scalar) {
    }

    /* (non-Javadoc)
     * @see goldengate.snmp.GgInterfaceMib#updateServices(org.snmp4j.agent.MOScope)
     */
    @Override
    public void updateServices(MOScope range) {
    }

}
