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
package org.waarp.openr66.protocol.http.adminssl;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * @author Frederic Bregier
 * 
 */
public class HttpSslPipelineFactory implements ChannelPipelineFactory {
    public boolean useHttpCompression = false;
    public boolean enableRenegotiation = false;

    public HttpSslPipelineFactory(boolean useHttpCompression,
            boolean enableRenegotiation) {
        this.useHttpCompression = useHttpCompression;
        this.enableRenegotiation = enableRenegotiation;
    }

    public ChannelPipeline getPipeline() {
        final ChannelPipeline pipeline = Channels.pipeline();
        // Add SSL handler first to encrypt and decrypt everything.
        SslHandler sslhandler = Configuration.waarpSslContextFactory.initPipelineFactory(true,
                false, enableRenegotiation);
        sslhandler.setIssueHandshake(true);
        pipeline.addLast("ssl", sslhandler);

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getHttpPipelineExecutor()));
        pipeline.addLast("streamer", new ChunkedWriteHandler());
        if (useHttpCompression) {
            pipeline.addLast("deflater", new HttpContentCompressor());
        }
        pipeline.addLast("handler", new HttpSslHandler());
        return pipeline;
    }
}
