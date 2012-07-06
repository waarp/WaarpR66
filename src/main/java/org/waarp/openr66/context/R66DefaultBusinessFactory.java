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

import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;

/**
 * Empty Business factory
 * 
 * @author Frederic Bregier
 * 
 */
public class R66DefaultBusinessFactory implements R66BusinessFactoryInterface {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(R66DefaultBusinessFactory.class);

	/*
	 * (non-Javadoc)
	 * @see
	 * org.waarp.openr66.context.R66BusinessFactoryInterface#getBusinessInterface(org.waarp.openr66
	 * .context.R66Session)
	 */
	@Override
	public R66BusinessInterface getBusinessInterface(R66Session session) {
		logger.debug("No Business");
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.waarp.openr66.context.R66BusinessFactoryInterface#releaseResources()
	 */
	@Override
	public void releaseResources() {
	}

}
