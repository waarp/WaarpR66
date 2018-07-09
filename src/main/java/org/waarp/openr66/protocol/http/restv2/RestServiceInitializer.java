/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */

package org.waarp.openr66.protocol.http.restv2;

import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInitializationException;
import org.waarp.openr66.protocol.http.restv2.handler.HostConfigHandler;
import org.waarp.openr66.protocol.http.restv2.handler.HostIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.HostsHandler;
import org.waarp.openr66.protocol.http.restv2.handler.LimitsHandler;
import org.waarp.openr66.protocol.http.restv2.handler.RuleIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.RulesHandler;
import org.waarp.openr66.protocol.http.restv2.handler.TransferIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.TransfersHandler;

import java.util.ArrayList;
import java.util.Collection;


//!\ For testing purposes only, DO NOT USE TO LAUNCH THE REST SERVICE /!\
public class RestServiceInitializer {

    private static final int TEST_PORT = 8080;

    public static void main(String[] args) throws OpenR66RestInitializationException, InterruptedException {

        Collection<HttpHandler> handlers = new ArrayList<HttpHandler>();
        handlers.add(new TransfersHandler());
        handlers.add(new TransferIdHandler());
        handlers.add(new HostConfigHandler());
        handlers.add(new HostsHandler());
        handlers.add(new HostIdHandler());
        handlers.add(new LimitsHandler());
        handlers.add(new RulesHandler());
        handlers.add(new RuleIdHandler());

        NettyHttpService restService = NettyHttpService.builder("WaarpR66-Rest")
                .setPort(TEST_PORT)
                .setHttpHandlers(handlers)
                .build();

        try {
            restService.start();
        } catch (Exception e) {
            throw new OpenR66RestInitializationException();
        }

        RestServiceInitializer r = new RestServiceInitializer();
        synchronized (r) {
            r.wait();
        }
    }
}
