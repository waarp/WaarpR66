/**
 * This file is part of Waarp Project (named also Waarp or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
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
package org.waarp.openr66.protocol.http.rest;

import static org.jboss.netty.channel.Channels.pipeline;

import org.waarp.common.crypto.ssl.WaarpSslContextFactory;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.networkhandler.NetworkServerPipelineFactory;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.GlobalTrafficShapingHandler;

/**
 * Pipeline Factory for Rest HTTP support for R66
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpRestR66PipelineFactory implements ChannelPipelineFactory {
    public boolean useHttpCompression = false;
    private final WaarpSslContextFactory waarpSslContextFactory;
    public static String CHUNKEDWRITER = "chunkedWriter";

    public HttpRestR66PipelineFactory(boolean useHttpCompression, WaarpSslContextFactory waarpSslContextFactory) {
        this.waarpSslContextFactory = waarpSslContextFactory;
        this.useHttpCompression = useHttpCompression;
    }

    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        // Enable HTTPS if necessary.
        if (waarpSslContextFactory != null) {
        	SslHandler handler = waarpSslContextFactory.initPipelineFactory(true,
                    waarpSslContextFactory.needClientAuthentication(), false);
        	handler.setIssueHandshake(true);
        	pipeline.addLast("ssl", handler);
        }
        
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        GlobalTrafficShapingHandler handler = Configuration.configuration.getGlobalTrafficShapingHandler();
        if (handler != null) {
            pipeline.addLast(NetworkServerPipelineFactory.LIMIT, handler);
        }
        ChannelTrafficShapingHandler trafficChannel = null;
        try {
            trafficChannel = Configuration.configuration.newChannelTrafficShapingHandler();
            if (trafficChannel != null) {
                pipeline.addLast(NetworkServerPipelineFactory.LIMITCHANNEL, trafficChannel);
            }
        } catch (OpenR66ProtocolNoDataException e) {
        }
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getHttpPipelineExecutor()));
        if (useHttpCompression) {
            pipeline.addLast("deflater", new HttpContentCompressor());
        }
        pipeline.addLast(CHUNKEDWRITER, new ChunkedWriteHandler());
        HttpRestR66Handler r66handler = new HttpRestR66Handler();
        // XXX FIXME default but should be able to change the default by configuration
        r66handler.checkAuthent = true;
        r66handler.checkTime = 0;
        pipeline.addLast("handler", r66handler);
        return pipeline;
    }
}
