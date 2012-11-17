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
package org.waarp.openr66.context;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import org.waarp.common.state.MachineState;
import org.waarp.common.state.Transition;

/**
 * Finite Dual State Machine for OpenR66 (Requester=R, requesteD=D, Sender=S, Receive=R)
 * 
 * @author Frederic Bregier
 * 
 */
public enum R66FiniteDualStates {
	OPENEDCHANNEL, CLOSEDCHANNEL, ERROR,
	STARTUP,
	AUTHENTR, AUTHENTD,
	REQUESTR, REQUESTD, VALID,
	DATAR, DATAS,
	ENDTRANSFERR, ENDREQUESTR,
	ENDTRANSFERS, ENDREQUESTS,
	TEST, INFORMATION, VALIDOTHER,
	SHUTDOWN, BUSINESSR, BUSINESSD;
	// not used in LSH
	// CONNECTERROR,
	// KEEPALIVEPACKET;

	private enum R66Transition {
		tOPENEDCHANNEL(OPENEDCHANNEL, EnumSet.of(STARTUP, CLOSEDCHANNEL, ERROR)),
		tSTARTUP(STARTUP, EnumSet.of(AUTHENTR, AUTHENTD, CLOSEDCHANNEL, ERROR)),
		tAUTHENTR(AUTHENTR, EnumSet.of(AUTHENTD, CLOSEDCHANNEL, ERROR)),
		tAUTHENTD(AUTHENTD, EnumSet.of(REQUESTR, VALIDOTHER, INFORMATION, SHUTDOWN, TEST,
				BUSINESSR, BUSINESSD, ENDREQUESTS, CLOSEDCHANNEL, ERROR)),
		tREQUESTR(REQUESTR, EnumSet.of(VALID, REQUESTD, CLOSEDCHANNEL, ERROR)),
		tREQUESTD(REQUESTD, EnumSet.of(VALID, DATAS, DATAR, CLOSEDCHANNEL, ERROR)),
		tVALID(VALID, EnumSet.of(REQUESTD, DATAR, CLOSEDCHANNEL, ERROR)),
		tDATAS(DATAS, EnumSet.of(DATAS, ENDTRANSFERS, CLOSEDCHANNEL, ERROR)),
		tDATAR(DATAR, EnumSet.of(DATAR, ENDTRANSFERS, CLOSEDCHANNEL, ERROR)),
		tENDTRANSFERS(ENDTRANSFERS, EnumSet.of(ENDTRANSFERR, CLOSEDCHANNEL, ERROR)),
		tENDTRANSFERR(ENDTRANSFERR, EnumSet.of(ENDREQUESTS, CLOSEDCHANNEL, ERROR)),
		tENDREQUESTS(ENDREQUESTS, EnumSet.of(ENDREQUESTR, CLOSEDCHANNEL, ERROR)),
		tENDREQUESTR(ENDREQUESTR, EnumSet.of(CLOSEDCHANNEL, ERROR)),
		tINFORMATION(INFORMATION, EnumSet.of(VALIDOTHER, CLOSEDCHANNEL, ERROR)),
		tTEST(TEST, EnumSet.of(TEST, VALIDOTHER)),
		tVALIDOTHER(VALIDOTHER, EnumSet.of(VALIDOTHER, CLOSEDCHANNEL, ERROR)),
		tSHUTDOWN(SHUTDOWN, EnumSet.of(CLOSEDCHANNEL, SHUTDOWN, ERROR)),
		tERROR(ERROR, EnumSet.of(ERROR, CLOSEDCHANNEL)),
		tCLOSEDCHANNEL(CLOSEDCHANNEL, EnumSet.noneOf(R66FiniteDualStates.class)),
		tBUSINESSR(BUSINESSR, EnumSet.of(ERROR, BUSINESSD, CLOSEDCHANNEL, VALIDOTHER)),
		tBUSINESSD(BUSINESSD, EnumSet.of(ERROR, BUSINESSD, BUSINESSR, CLOSEDCHANNEL, VALIDOTHER));

		public Transition<R66FiniteDualStates> elt;

		private R66Transition(R66FiniteDualStates state, EnumSet<R66FiniteDualStates> set) {
			this.elt = new Transition<R66FiniteDualStates>(state, set);
		}
	}

	private static ConcurrentHashMap<R66FiniteDualStates, EnumSet<?>> stateMap =
			new ConcurrentHashMap<R66FiniteDualStates, EnumSet<?>>();

	/**
	 * This method should be called once at startup to initialize the Finite States association.
	 */
	public static void initR66FiniteStates() {
		for (R66Transition trans : R66Transition.values()) {
			stateMap.put(trans.elt.state, trans.elt.set);
		}
	}

	/**
	 * 
	 * @return a new Session MachineState for OpenR66
	 */
	public static MachineState<R66FiniteDualStates> newSessionMachineState() {
		MachineState<R66FiniteDualStates> machine =
				new MachineState<R66FiniteDualStates>(OPENEDCHANNEL, stateMap);
		return machine;
	}

	/**
	 * 
	 * @param machine
	 *            the Session MachineState to release
	 */
	public static void endSessionMachineSate(MachineState<R66FiniteDualStates> machine) {
		machine.release();
	}
}
