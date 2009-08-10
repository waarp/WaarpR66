/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.protocol.networkhandler.ssl;

import javax.net.ssl.SSLEngine;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.networkhandler.packet.NetworkPacketCodec;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.traffic.GlobalTrafficShapingHandler;

/**
 * NetworkServer pipeline for SSL
 *
 * @author Frederic Bregier
 */
public class NetworkSslServerPipelineFactory implements ChannelPipelineFactory {
    private final boolean isClient;

    /**
     *
     * @param isClient
     *            True if this Factory is to be used in Client mode
     */
    public NetworkSslServerPipelineFactory(boolean isClient) {
        super();
        this.isClient = isClient;
    }

    @Override
    public ChannelPipeline getPipeline() {
        final ChannelPipeline pipeline = Channels.pipeline();
        // Add SSL handler first to encrypt and decrypt everything.
        // You will need something more complicated to identify both
        // and server in the real world.
        SSLEngine engine;
        if (isClient) {
            engine = SecureSslContextFactory.getClientContext()
                    .createSSLEngine();
            engine.setUseClientMode(true);
        } else {
            engine = SecureSslContextFactory.getServerContext()
                    .createSSLEngine();
            engine.setUseClientMode(false);
            engine.setNeedClientAuth(true);
        }
        pipeline.addLast("ssl", new SslHandler(engine));

        pipeline.addLast("codec", new NetworkPacketCodec());
        GlobalTrafficShapingHandler handler = Configuration.configuration
                .getGlobalTrafficShapingHandler();
        if (handler != null) {
            pipeline.addLast("LIMIT", handler);
            pipeline.addLast("LIMITCHANNEL", Configuration.configuration
                    .newChannelTrafficShapingHandler());
        }
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getServerPipelineExecutor()));
        pipeline.addLast("handler", new NetworkSslServerHandler());
        return pipeline;
    }

}
