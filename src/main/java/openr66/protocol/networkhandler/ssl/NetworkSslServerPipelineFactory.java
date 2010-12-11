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
package openr66.protocol.networkhandler.ssl;

import goldengate.common.crypto.ssl.GgSecureKeyStore;
import goldengate.common.crypto.ssl.GgSslContextFactory;

import java.util.concurrent.ExecutorService;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.networkhandler.packet.NetworkPacketCodec;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.GlobalTrafficShapingHandler;

/**
 * @author Frederic Bregier
 *
 */
public class NetworkSslServerPipelineFactory implements ChannelPipelineFactory {
    private final boolean isClient;
    public static GgSslContextFactory ggSslContextFactory;
    public static GgSecureKeyStore ggSecureKeyStore;
    private final ExecutorService executorService;

    /**
     *
     * @param isClient
     *            True if this Factory is to be used in Client mode
     */
    public NetworkSslServerPipelineFactory(boolean isClient, ExecutorService executor) {
        super();
        this.isClient = isClient;
        this.executorService = executor;
    }

    @Override
    public ChannelPipeline getPipeline() {
        final ChannelPipeline pipeline = Channels.pipeline();
        // Add SSL handler first to encrypt and decrypt everything.
        if (isClient) {
            // Not server: no clientAuthent, no renegotiation
            pipeline.addLast("ssl",
                ggSslContextFactory.initPipelineFactory(false,
                        false, false, executorService));
        } else {
            // Server: no renegotiation still, but possible clientAuthent
            pipeline.addLast("ssl",
                    ggSslContextFactory.initPipelineFactory(true,
                            ggSslContextFactory.needClientAuthentication(),
                            false, executorService));
        }

        pipeline.addLast("codec", new NetworkPacketCodec());
        GlobalTrafficShapingHandler handler = Configuration.configuration
                .getGlobalTrafficShapingHandler();
        if (handler != null) {
            pipeline.addLast("LIMIT", handler);
            ChannelTrafficShapingHandler trafficChannel = null;
            try {
                trafficChannel =
                    Configuration.configuration
                    .newChannelTrafficShapingHandler();
                pipeline.addLast("LIMITCHANNEL", trafficChannel);
            } catch (OpenR66ProtocolNoDataException e) {
            }
        }
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getServerPipelineExecutor()));
        pipeline.addLast("handler", new NetworkSslServerHandler(!this.isClient));
        return pipeline;
    }
}
