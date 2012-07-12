/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.snmp;

import goldengate.snmp.interf.GgCounter32;
import openr66.protocol.configuration.Configuration;

/**
 * R66 Counter32 SNMP implementation
 * 
 * @author Frederic Bregier
 * 
 */
public class R66Counter32 extends GgCounter32 {

	private int type = 1;
	private int entry = 0;
	/**
     * 
     */
	private static final long serialVersionUID = -5850987508703222927L;

	public R66Counter32(int type, int entry) {
		this.type = type;
		this.entry = entry;
		setInternalValue();
	}

	public R66Counter32(int type, int entry, long value) {
		this.type = type;
		this.entry = entry;
		setInternalValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see goldengate.snmp.interf.GgGauge32#setInternalValue()
	 */
	@Override
	protected void setInternalValue() {
		Configuration.configuration.monitoring.run(type, entry);
	}

	/*
	 * (non-Javadoc)
	 * @see goldengate.snmp.interf.GgGauge32#setInternalValue(long)
	 */
	@Override
	protected void setInternalValue(long value) {
		setValue(value);
	}

}
