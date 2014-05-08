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
package org.waarp.openr66.protocol.http.rest.test;

import java.io.File;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;
import org.waarp.openr66.server.R66Server;

/**
 * @author "Frederic Bregier"
 *
 */
public class HttpTestR66PseudoMain {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
        final WaarpInternalLogger logger = WaarpInternalLoggerFactory
                .getLogger(HttpTestR66PseudoMain.class);
        String pathTemp = "J:/Temp/temp";
        if (!R66Server.initialize(args[0])) {
        	System.err.println("Error during startup");
        	System.exit(1);
        }
        HttpRestR66Handler.initializeService(10000, 8080, pathTemp, new File("J:/Temp/temp/key.sha256"));
		logger.warn("Server RestOpenR66 starts");
		/* HmacSha256 sha = new HmacSha256();
		sha.generateKey();
		sha.saveSecretKey(new File("J:/Temp/temp/key.sha256"));
		*/
	}

}
