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
package openr66.context;

import goldengate.common.state.MachineState;
import goldengate.common.state.Transition;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

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
    SHUTDOWN; 
    // not used in LSH
    // CONNECTERROR, 
    // KEEPALIVEPACKET;
    
    private enum R66Transition {
        tOPENEDCHANNEL(OPENEDCHANNEL, EnumSet.of(STARTUP, ERROR)),
        tSTARTUP(STARTUP, EnumSet.of(AUTHENTR, ERROR)),
        tAUTHENTR(AUTHENTR, EnumSet.of(AUTHENTD, ERROR)),
        tAUTHENTD(AUTHENTD, EnumSet.of(REQUESTR, VALIDOTHER, INFORMATION, SHUTDOWN, TEST, ERROR)),
        tREQUESTR(REQUESTR, EnumSet.of(VALID, REQUESTD, ERROR)),
        tREQUESTD(REQUESTD, EnumSet.of(DATAS, DATAR, ERROR)),
        tVALID(VALID, EnumSet.of(REQUESTD, DATAR, ERROR)),
        tDATAS(DATAS, EnumSet.of(DATAS, ENDTRANSFERS, ERROR)),
        tDATAR(DATAR, EnumSet.of(DATAR, ENDTRANSFERS, ERROR)),
        tENDTRANSFERS(ENDTRANSFERS, EnumSet.of(ENDTRANSFERR, ERROR)),
        tENDTRANSFERR(ENDTRANSFERR, EnumSet.of(ENDREQUESTS, ERROR)),
        tENDREQUESTS(ENDREQUESTS, EnumSet.of(ENDREQUESTR, ERROR)),
        tENDREQUESTR(ENDREQUESTR, EnumSet.of(CLOSEDCHANNEL, ERROR)),
        tINFORMATION(INFORMATION, EnumSet.of(VALIDOTHER, CLOSEDCHANNEL, ERROR)),
        tTEST(TEST, EnumSet.of(TEST, VALIDOTHER)),
        tVALIDOTHER(VALIDOTHER, EnumSet.of(VALIDOTHER,CLOSEDCHANNEL, ERROR)),
        tSHUTDOWN(SHUTDOWN, EnumSet.of(CLOSEDCHANNEL, SHUTDOWN, ERROR)),
        tERROR(ERROR, EnumSet.of(ERROR, CLOSEDCHANNEL)),
        tCLOSEDCHANNEL(CLOSEDCHANNEL, EnumSet.noneOf(R66FiniteDualStates.class));
        
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
        for (R66Transition trans: R66Transition.values()) {
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
     * @param machine the Session MachineState to release
     */
    public static void endSessionMachineSate(MachineState<R66FiniteDualStates> machine) {
        machine.release();
        machine = null;
    }
}
