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
 * Finite State Machine for OpenR66
 * 
 * @author Frederic Bregier
 *
 */
public enum R66FiniteStates {
    OPENEDCHANNEL, CLOSEDCHANNEL, ERROR,
    STARTUP, AUTHENT,
    REQUEST, VALID, DATA, ENDTRANSFER, ENDREQUEST, 
    TEST, INFORMATION, VALIDOTHER, 
    SHUTDOWN; 
    // not used in LSH
    // CONNECTERROR, 
    // KEEPALIVEPACKET;
    
    private enum R66Transition {
        tOPENEDCHANNEL(OPENEDCHANNEL, EnumSet.of(STARTUP, ERROR)),
        tSTARTUP(STARTUP, EnumSet.of(AUTHENT, ERROR)),
        tAUTHENT(AUTHENT, EnumSet.of(VALIDOTHER, REQUEST, INFORMATION, SHUTDOWN, ERROR)),
        tREQUEST(REQUEST, EnumSet.of(DATA, ERROR)),
        tVALID(VALID, EnumSet.of(REQUEST, DATA, ERROR)),
        tDATA(DATA, EnumSet.of(DATA, ENDTRANSFER, ERROR)),
        tENDTRANSFER(ENDTRANSFER, EnumSet.of(ENDREQUEST, ERROR)),
        tENDREQUEST(ENDREQUEST, EnumSet.of(CLOSEDCHANNEL, ERROR)),
        tINFORMATION(INFORMATION, EnumSet.of(VALIDOTHER, CLOSEDCHANNEL, ERROR)),
        tTEST(TEST, EnumSet.of(TEST, VALIDOTHER)),
        tVALIDOTHER(VALIDOTHER, EnumSet.of(CLOSEDCHANNEL, ERROR)),
        tSHUTDOWN(SHUTDOWN, EnumSet.of(CLOSEDCHANNEL, ERROR)),
        tERROR(ERROR, EnumSet.of(ERROR, CLOSEDCHANNEL)),
        tCLOSEDCHANNEL(CLOSEDCHANNEL, EnumSet.noneOf(R66FiniteStates.class));
        
        public Transition<R66FiniteStates> elt;
        private R66Transition(R66FiniteStates state, EnumSet<R66FiniteStates> set) {
            this.elt = new Transition<R66FiniteStates>(state, set);
        }
    }
    
    private static ConcurrentHashMap<R66FiniteStates, EnumSet<?>> stateMap =
        new ConcurrentHashMap<R66FiniteStates, EnumSet<?>>();
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
    public static MachineState<R66FiniteStates> newSessionMachineState() {
        MachineState<R66FiniteStates> machine = 
            new MachineState<R66FiniteStates>(OPENEDCHANNEL, stateMap);
        return machine;
    }
    /**
     * 
     * @param machine the Session MachineState to release
     */
    public static void endSessionMachineSate(MachineState<R66FiniteStates> machine) {
        machine.release();
    }
}
