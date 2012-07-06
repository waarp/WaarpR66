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
package org.waarp.openr66.protocol.exception;

/**
 * Protocol exception on Business part due to Query is already finished
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessQueryAlreadyFinishedException extends
		OpenR66ProtocolBusinessException {
	/**
     *
     */
	private static final long serialVersionUID = 1014687763768508552L;

	/**
	 *
	 */
	public OpenR66ProtocolBusinessQueryAlreadyFinishedException() {
		super();
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public OpenR66ProtocolBusinessQueryAlreadyFinishedException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * @param arg0
	 */
	public OpenR66ProtocolBusinessQueryAlreadyFinishedException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public OpenR66ProtocolBusinessQueryAlreadyFinishedException(Throwable arg0) {
		super(arg0);
	}

}
