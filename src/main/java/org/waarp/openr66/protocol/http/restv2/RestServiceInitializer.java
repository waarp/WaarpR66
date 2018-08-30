/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.waarp.openr66.protocol.http.restv2;

import co.cask.http.AbstractHttpHandler;
import co.cask.http.ChannelPipelineModifier;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import org.waarp.common.crypto.HmacSha256;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.protocol.http.restv2.handler.AbstractRestHttpHandler;
import org.waarp.openr66.protocol.http.restv2.handler.HostConfigHandler;
import org.waarp.openr66.protocol.http.restv2.handler.HostIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.HostsHandler;
import org.waarp.openr66.protocol.http.restv2.handler.LimitsHandler;
import org.waarp.openr66.protocol.http.restv2.handler.RuleIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.RulesHandler;
import org.waarp.openr66.protocol.http.restv2.handler.ServerHandler;
import org.waarp.openr66.protocol.http.restv2.handler.TransferIdHandler;
import org.waarp.openr66.protocol.http.restv2.handler.TransfersHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public final class RestServiceInitializer {

    private RestServiceInitializer() {
        throw new UnsupportedOperationException("'RestServiceInitializer' cannot be instantiated.");
    }

    public static final Collection<AbstractRestHttpHandler> handlers;

    static {
        handlers = new ArrayList<AbstractRestHttpHandler>();
        handlers.add(new TransfersHandler());
        handlers.add(new TransferIdHandler());
        handlers.add(new HostConfigHandler());
        handlers.add(new HostsHandler());
        handlers.add(new HostIdHandler());
        handlers.add(new LimitsHandler());
        handlers.add(new RulesHandler());
        handlers.add(new RuleIdHandler());
        handlers.add(new ServerHandler());
    }

    public static NettyHttpService initRestService(RestConfiguration config) {
        NettyHttpService restService = NettyHttpService.builder("WaarpR66-Rest v2")
                .setPort(config.REST_PORT)
                .setHost(config.REST_ADDRESS)
                .setHttpHandlers(handlers)
                .setHandlerHooks(Collections.singleton(new RestHandlerHook(config.REST_AUTHENTICATED,
                        config.REST_SIGNATURE, config.hmacSha256, config.REST_TIME_LIMIT)))
                .setExceptionHandler(new RestExceptionHandler())
                /* Adds the routing error handler to the service pipeline. */
                .setChannelPipelineModifier(new ChannelPipelineModifier() {
                    @Override
                    public void modify(ChannelPipeline channelPipeline) {
                        channelPipeline.addBefore("router", "errorHandler", new RestRoutingErrorHandler());
                        channelPipeline.remove("compressor");
                    }
                })
                .setExecThreadKeepAliveSeconds(-1L)
                .build();
        try {
            restService.start();
            return restService;
        } catch (Throwable e) {
            throw new RuntimeException();
        }
    }

    //!\ For testing purposes only, DO NOT USE TO LAUNCH THE R66 SERVER /!\
    public static void main(String[] args) throws InterruptedException {

        RestConfiguration config = new RestConfiguration();
        config.REST_PORT = 8088;
        config.REST_ADDRESS = "0.0.0.0";
        config.REST_AUTHENTICATED = true;
        config.REST_SIGNATURE = false;
        config.hmacSha256 = new HmacSha256();
        NettyHttpService restService = initRestService(config);

        synchronized (restService) {
            restService.wait();
        }
    }
}
