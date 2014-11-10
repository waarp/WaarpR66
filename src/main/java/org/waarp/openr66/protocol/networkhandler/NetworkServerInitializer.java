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
package org.waarp.openr66.protocol.networkhandler;

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.HashedWheelTimer;

import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.networkhandler.packet.NetworkPacketCodec;

/**
 * NetworkServer pipeline (Requester side)
 * 
 * @author Frederic Bregier
 */
public class NetworkServerInitializer extends ChannelInitializer<SocketChannel> {
    /**
     * Global HashedWheelTimer
     */
    public HashedWheelTimer timer = (HashedWheelTimer) Configuration.configuration.getTimerClose();

    public static final String TIMEOUT = "timeout";
    public static final String READTIMEOUT = "readTimeout";
    public static final String LIMIT = "LIMIT";
    public static final String LIMITCHANNEL = "LIMITCHANNEL";

    protected boolean server = false;

    public NetworkServerInitializer(boolean server) {
        this.server = server;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("codec", new NetworkPacketCodec());
        pipeline.addLast(TIMEOUT, new IdleStateHandler(0, 0, Configuration.configuration.TIMEOUTCON,
                TimeUnit.MILLISECONDS));
        GlobalTrafficShapingHandler handler = Configuration.configuration.getGlobalTrafficShapingHandler();
        if (handler != null) {
            pipeline.addLast(LIMIT, handler);
        }
        ChannelTrafficShapingHandler trafficChannel = null;
        try {
            trafficChannel = Configuration.configuration.newChannelTrafficShapingHandler();
            if (trafficChannel != null) {
                pipeline.addLast(LIMITCHANNEL, trafficChannel);
            }
        } catch (OpenR66ProtocolNoDataException e) {
        }
        pipeline.addLast(Configuration.configuration.getHandlerGroup(), "handler",
                new NetworkServerHandler(this.server));
    }

}
