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

import static io.netty.channel.Channels.pipeline;

import org.waarp.common.crypto.ssl.WaarpSslContextFactory;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.protocol.configuration.Configuration;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelInitializer<SocketChannel>;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.execution.ExecutionHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * Pipeline Factory for Rest HTTP support for R66
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpRestR66Initializer implements ChannelInitializer<SocketChannel> {
	private final boolean useHttpCompression;
    private final WaarpSslContextFactory waarpSslContextFactory;
    private final RestConfiguration restConfiguration;
    
    public HttpRestR66Initializer(boolean useHttpCompression, WaarpSslContextFactory waarpSslContextFactory, RestConfiguration configuration) {
        this.waarpSslContextFactory = waarpSslContextFactory;
        this.useHttpCompression = useHttpCompression;
        this.restConfiguration = configuration;
    }

    protected void initChannel(Channel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        // Enable HTTPS if necessary.
        if (waarpSslContextFactory != null) {
        	SslHandler handler = waarpSslContextFactory.initInitializer(true,
                    false, false);
        	handler.setIssueHandshake(true);
        	pipeline.addLast("ssl", handler);
        }
        
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        /*
        GlobalTrafficShapingHandler handler = Configuration.configuration.getGlobalTrafficShapingHandler();
        if (handler != null) {
            pipeline.addLast(NetworkServerInitializer.LIMIT, handler);
        }
        ChannelTrafficShapingHandler trafficChannel = null;
        try {
            trafficChannel = Configuration.configuration.newChannelTrafficShapingHandler();
            if (trafficChannel != null) {
                pipeline.addLast(NetworkServerInitializer.LIMITCHANNEL, trafficChannel);
            }
        } catch (OpenR66ProtocolNoDataException e) {
        }
        */
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getHttpPipelineExecutor()));
        if (useHttpCompression) {
            pipeline.addLast("deflater", new HttpContentCompressor());
        }
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
        HttpRestR66Handler r66handler = new HttpRestR66Handler(restConfiguration);
        pipeline.addLast("handler", r66handler);
        return pipeline;
    }
}
