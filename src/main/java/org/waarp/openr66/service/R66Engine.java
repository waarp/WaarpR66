/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.service;

import org.waarp.common.future.WaarpFuture;
import org.waarp.common.service.EngineAbstract;

/**
 * Engine used to start and stop the real R66 service
 * @author Frederic Bregier
 *
 */
public class R66Engine extends EngineAbstract {
	protected WaarpFuture closeFurure = new WaarpFuture(true);
	
	@Override
	public void run() {
		System.err.println("Service started");
	}

	@Override
	public void shutdown() {
		closeFurure.setSuccess();
		System.err.println("Service stopped");
	}

	@Override
	public boolean isShutdown() {
		return closeFurure.isDone();
	}

	@Override
	public WaarpFuture getShutdownFuture() {
		return closeFurure;
	}
}
