/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.snmp;

import org.snmp4j.agent.DuplicateRegistrationException;
import org.snmp4j.agent.MOScope;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.VariableBinding;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket.TRANSFERMODE;
import org.waarp.openr66.protocol.utils.Version;
import org.waarp.snmp.r66.WaarpPrivateMib;
import org.waarp.snmp.utils.MemoryGauge32;
import org.waarp.snmp.utils.MemoryGauge32.MemoryType;
import org.waarp.snmp.utils.WaarpMORow;
import org.waarp.snmp.utils.WaarpMOScalar;
import org.waarp.snmp.utils.WaarpUptime;

/**
 * Waarp OpenR66 Private MIB implementation
 * 
 * @author Frederic Bregier
 * 
 */
public class R66PrivateMib extends WaarpPrivateMib {
	/**
	 * Internal Logger
	 */
	private static WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(R66PrivateMib.class);

	/**
	 * @param sysdesc
	 * @param port
	 * @param smiPrivateCodeFinal
	 * @param typeWaarpObject
	 * @param scontactName
	 * @param stextualName
	 * @param saddress
	 * @param iservice
	 */
	public R66PrivateMib(String sysdesc, int port, int smiPrivateCodeFinal,
			int typeWaarpObject, String scontactName, String stextualName,
			String saddress, int iservice) {
		super(sysdesc, port, smiPrivateCodeFinal, typeWaarpObject,
				scontactName, stextualName, saddress, iservice);
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.snmp.WaarpPrivateMib#agentRegisterWaarpMib()
	 */
	@Override
	protected void agentRegisterWaarpMib()
			throws DuplicateRegistrationException {
		logger.debug("registerGGMib");
		// register Static info
		rowInfo = new WaarpMORow(this, rootOIDWaarpInfo, WaarpDefinition,
				MibLevel.staticInfo.ordinal());
		rowInfo.setValue(WaarpDefinitionIndex.applName.ordinal(),
				"Waarp OpenR66");
		rowInfo.setValue(WaarpDefinitionIndex.applServerName.ordinal(),
				Configuration.configuration.HOST_ID);
		rowInfo.setValue(WaarpDefinitionIndex.applVersion.ordinal(),
				Version.ID);
		rowInfo.setValue(WaarpDefinitionIndex.applDescription.ordinal(),
				"Waarp OpenR66: File Transfer Monitor");
		rowInfo.setValue(WaarpDefinitionIndex.applURL.ordinal(),
				"http://org.waarp.openr66.free.fr");
		rowInfo.setValue(WaarpDefinitionIndex.applApplicationProtocol.ordinal(),
				applicationProtocol);

		rowInfo.registerMOs(agent.getServer(), null);
		// register General info
		rowGlobal = new WaarpMORow(this, rootOIDWaarpGlobal, WaarpGlobalValues,
				MibLevel.globalInfo.ordinal());
		WaarpMOScalar memoryScalar = rowGlobal.row[WaarpGlobalValuesIndex.memoryTotal.ordinal()];
		memoryScalar.setValue(new MemoryGauge32(MemoryType.TotalMemory));
		memoryScalar = rowGlobal.row[WaarpGlobalValuesIndex.memoryFree.ordinal()];
		memoryScalar.setValue(new MemoryGauge32(MemoryType.FreeMemory));
		memoryScalar = rowGlobal.row[WaarpGlobalValuesIndex.memoryUsed.ordinal()];
		memoryScalar.setValue(new MemoryGauge32(MemoryType.UsedMemory));
		rowGlobal.registerMOs(agent.getServer(), null);
		// setup UpTime to SysUpTime and change status
		scalarUptime = rowGlobal.row[WaarpGlobalValuesIndex.applUptime.ordinal()];
		scalarUptime.setValue(new WaarpUptime(upTime));
		changeStatus(OperStatus.restarting);
		changeStatus(OperStatus.up);
		// register Detailed info
		rowDetailed = new WaarpMORow(this, rootOIDWaarpDetailed, WaarpDetailedValues,
				MibLevel.detailedInfo.ordinal());
		rowDetailed.registerMOs(agent.getServer(), null);
		// register Error info
		rowError = new WaarpMORow(this, rootOIDWaarpError, WaarpErrorValues,
				MibLevel.errorInfo.ordinal());
		rowError.registerMOs(agent.getServer(), null);
	}

	/**
	 * Send a notification (trap or inform) for Shutdown event
	 * 
	 * @param message
	 * @param message2
	 */
	public void notifyStartStop(String message, String message2) {
		if (!TrapLevel.StartStop.isLevelValid(agent.trapLevel))
			return;
		notify(NotificationElements.TrapShutdown, message, message2);
	}

	/**
	 * Send a notification (trap or inform) for Error event
	 * 
	 * @param message
	 * @param message2
	 */
	public void notifyError(String message, String message2) {
		if (!TrapLevel.Alert.isLevelValid(agent.trapLevel))
			return;
		notify(NotificationElements.TrapError, message, message2);
	}

	/**
	 * Send a notification (trap or inform) for Server Overloaded event
	 * 
	 * @param message
	 * @param message2
	 */
	public void notifyOverloaded(String message, String message2) {
		if (!TrapLevel.Warning.isLevelValid(agent.trapLevel))
			return;
		notify(NotificationElements.TrapOverloaded, message, message2);
	}

	/**
	 * Send a notification (trap or inform) for Warning event
	 * 
	 * @param message
	 * @param message2
	 */
	public void notifyWarning(String message, String message2) {
		if (!TrapLevel.Warning.isLevelValid(agent.trapLevel))
			return;
		notify(NotificationElements.TrapWarning, message, message2);
	}

	/**
	 * Send a notification (trap or inform) for Warning/Error event on a single Transfer Task
	 * 
	 * @param message
	 * @param runner
	 */
	public void notifyInfoTask(String message, DbTaskRunner runner) {
		if (!TrapLevel.All.isLevelValid(agent.trapLevel))
			return;
		if (logger.isDebugEnabled())
			logger.debug("Notify: " + NotificationElements.InfoTask + ":" + message +
					":" + runner.toShortString());
		long delay = (runner.getStart().getTime() -
				agent.getUptimeSystemTime()) / 10;
		if (delay < 0)
			delay = 0;
		agent.getNotificationOriginator()
				.notify(new OctetString("public"),
						NotificationElements.InfoTask
								.getOID(rootOIDWaarpNotif),
						new VariableBinding[] {
								new VariableBinding(
										NotificationElements.InfoTask.getOID(
												rootOIDWaarpNotif, 1),
										new OctetString(
												NotificationElements.InfoTask
														.name())),
								new VariableBinding(
										NotificationElements.InfoTask.getOID(
												rootOIDWaarpNotif, 1),
										new OctetString(message)),
								// Start of Task
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.globalStepInfo
																.getOID()),
										new Gauge32(runner.getGloballaststep())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.stepInfo
																.getOID()),
										new Gauge32(runner.getStep() + 1)),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.rankFileInfo
																.getOID()),
										new Gauge32(runner.getRank())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.stepStatusInfo
																.getOID()),
										new OctetString(runner.getStatus().mesg)),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.filenameInfo
																.getOID()),
										new OctetString(runner.getFilename())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.originalNameInfo
																.getOID()),
										new OctetString(runner.getOriginalFilename())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.idRuleInfo
																.getOID()),
										new OctetString(runner.getRuleId())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.modeTransInfo
																.getOID()),
										new OctetString(TRANSFERMODE.values()[runner.getMode()]
												.name())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.retrieveModeInfo
																.getOID()),
										new OctetString(runner.isSender() ? "Sender" : "Receiver")),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.startTransInfo
																.getOID()),
										new TimeTicks(delay)),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.infoStatusInfo
																.getOID()),
										new OctetString(runner.getErrorInfo().mesg)),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.requesterInfo
																.getOID()),
										new OctetString(runner.getRequester())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.requestedInfo
																.getOID()),
										new OctetString(runner.getRequested())),
								new VariableBinding(
										NotificationElements.InfoTask
												.getOID(rootOIDWaarpNotif,
														NotificationTasks.specialIdInfo
																.getOID()),
										new OctetString("" + runner.getSpecialId())),
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
	 * 
	 * @param element
	 * @param message
	 * @param message2
	 */
	private void notify(NotificationElements element, String message, String message2) {
		if (logger.isDebugEnabled())
			logger.debug("Notify: " + element + ":" + message + ":" + message2);
		agent.getNotificationOriginator().notify(
				new OctetString("public"),
				element.getOID(rootOIDWaarpNotif),
				new VariableBinding[] {
						new VariableBinding(
								element.getOID(rootOIDWaarpNotif, 1),
								new OctetString(element.name())),
						new VariableBinding(
								element.getOID(rootOIDWaarpNotif, 1),
								new OctetString(message)),
						new VariableBinding(
								element.getOID(rootOIDWaarpNotif, 1),
								new OctetString(message2)),
						new VariableBinding(SnmpConstants.sysDescr, snmpv2.getDescr()),
						new VariableBinding(SnmpConstants.sysObjectID, snmpv2.getObjectID()),
						new VariableBinding(SnmpConstants.sysContact, snmpv2.getContact()),
						new VariableBinding(SnmpConstants.sysName, snmpv2.getName()),
						new VariableBinding(SnmpConstants.sysLocation, snmpv2.getLocation())
				});
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.snmp.WaarpInterfaceMib#updateServices(org.waarp.snmp.WaarpMOScalar)
	 */
	@Override
	public void updateServices(WaarpMOScalar scalar) {
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.snmp.WaarpInterfaceMib#updateServices(org.snmp4j.agent.MOScope)
	 */
	@Override
	public void updateServices(MOScope range) {
	}

}
