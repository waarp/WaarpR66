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

import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.localhandler.packet.RequestPacket.TRANSFERMODE;
import openr66.protocol.utils.Version;

import org.snmp4j.agent.DuplicateRegistrationException;
import org.snmp4j.agent.MOScope;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.VariableBinding;

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
    
    public void notifyStartStop(String message, String message2) {
        if (!TrapLevel.StartStop.isLevelValid(agent.trapLevel))
            return;
        notify(NotificationElements.TrapShutdown, message, message2);
    }
    public void notifyError(String message, String message2) {
        if (!TrapLevel.Alert.isLevelValid(agent.trapLevel))
            return;
        notify(NotificationElements.TrapError, message, message2);
    }
    public void notifyOverloaded(String message, String message2) {
        if (!TrapLevel.Warning.isLevelValid(agent.trapLevel))
            return;
        notify(NotificationElements.TrapOverloaded, message, message2);
    }
    public void notifyWarning(String message, String message2) {
        if (!TrapLevel.Warning.isLevelValid(agent.trapLevel))
            return;
        notify(NotificationElements.TrapWarning, message, message2);
    }

    public void notifyInfoTask(String message, DbTaskRunner runner) {
        if (!TrapLevel.All.isLevelValid(agent.trapLevel)) return;
        if (logger.isDebugEnabled())
            logger.debug("Notify: " + NotificationElements.InfoTask + ":" + message +
                ":" + runner.toShortString());
        long delay = (runner.getStart().getTime()-
                agent.getUptimeSystemTime())/10;
        if (delay <0)
            delay = 0;
        agent.getNotificationOriginator()
                .notify(new OctetString("public"),
                        NotificationElements.InfoTask
                                .getOID(rootOIDGoldenGateNotif),
                        new VariableBinding[] {
                                new VariableBinding(
                                        NotificationElements.InfoTask.getOID(
                                                rootOIDGoldenGateNotif, 1),
                                        new OctetString(
                                                NotificationElements.InfoTask
                                                        .name())),
                                new VariableBinding(
                                        NotificationElements.InfoTask.getOID(
                                                rootOIDGoldenGateNotif, 1),
                                        new OctetString(message)),
                                // Start of Task
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.globalStepInfo
                                                                .getOID()),
                                        new Gauge32(runner.getGloballaststep())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.stepInfo
                                                                .getOID()),
                                        new Gauge32(runner.getStep())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.rankFileInfo
                                                                .getOID()),
                                        new Gauge32(runner.getRank())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.stepStatusInfo
                                                                .getOID()),
                                        new OctetString(runner.getStatus().mesg)),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.filenameInfo
                                                                .getOID()),
                                        new OctetString(runner.getFilename())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.originalNameInfo
                                                                .getOID()),
                                        new OctetString(runner.getOriginalFilename())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.idRuleInfo
                                                                .getOID()),
                                        new OctetString(runner.getRuleId())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.modeTransInfo
                                                                .getOID()),
                                        new OctetString(TRANSFERMODE.values()[runner.getMode()].name())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.retrieveModeInfo
                                                                .getOID()),
                                        new OctetString(runner.isSender()?"Sender":"Receiver")),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.startTransInfo
                                                                .getOID()),
                                        new TimeTicks(delay)),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.infoStatusInfo
                                                                .getOID()),
                                        new OctetString(runner.getErrorInfo().mesg)),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.requesterInfo
                                                                .getOID()),
                                        new OctetString(runner.getRequester())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.requestedInfo
                                                                .getOID()),
                                        new OctetString(runner.getRequested())),
                                new VariableBinding(
                                        NotificationElements.InfoTask
                                                .getOID(rootOIDGoldenGateNotif,
                                                        NotificationTasks.specialIdInfo
                                                                .getOID()),
                                        new OctetString(""+runner.getSpecialId())),
                                // End of Task
                                new VariableBinding(SnmpConstants.sysDescr,
                                        snmpv2.getDescr()),
                                new VariableBinding(SnmpConstants.sysObjectID,
                                        snmpv2.getObjectID()),
                                new VariableBinding(SnmpConstants.sysContact,
                                        snmpv2.getContact()),
                                new VariableBinding(SnmpConstants.sysName,
                                        snmpv2.getName()),
                                new VariableBinding(SnmpConstants.sysLocation,
                                        snmpv2.getLocation()) });
    }
    /**
     * Trap/Notification
     * @param element
     * @param message
     * @param message2
     */
    private void notify(NotificationElements element, String message, String message2) {
        if (logger.isDebugEnabled())
            logger.debug("Notify: "+element+":"+message+":"+message2);
        agent.getNotificationOriginator().notify(
                new OctetString("public"), 
                element.getOID(rootOIDGoldenGateNotif),
                new VariableBinding[] {
                    new VariableBinding(
                            element.getOID(rootOIDGoldenGateNotif, 1),
                            new OctetString(element.name())),
                    new VariableBinding(
                            element.getOID(rootOIDGoldenGateNotif, 1),
                            new OctetString(message)),
                    new VariableBinding(
                            element.getOID(rootOIDGoldenGateNotif, 1), 
                            new OctetString(message2)),
                    new VariableBinding(SnmpConstants.sysDescr, snmpv2.getDescr()),
                    new VariableBinding(SnmpConstants.sysObjectID, snmpv2.getObjectID()),
                    new VariableBinding(SnmpConstants.sysContact, snmpv2.getContact()),
                    new VariableBinding(SnmpConstants.sysName, snmpv2.getName()),
                    new VariableBinding(SnmpConstants.sysLocation, snmpv2.getLocation())
            });
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
