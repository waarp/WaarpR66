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
package org.waarp.openr66.protocol.http;

import static io.netty.channel.Channels.pipeline;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelInitializer<SocketChannel>;
import io.netty.handler.codec.http.HttpChunkAggregator;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.execution.ExecutionHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Pipeline Factory for HTTP support
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpInitializer implements ChannelInitializer<SocketChannel> {
	public boolean useHttpCompression = false;

	public HttpInitializer(boolean useHttpCompression) {
		this.useHttpCompression = useHttpCompression;
	}

	protected void initChannel(Channel ch) throws Exception {
		// Create a default pipeline implementation.
		ChannelPipeline pipeline = pipeline();

		pipeline.addLast("decoder", new HttpRequestDecoder());
		pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
		pipeline.addLast("encoder", new HttpResponseEncoder());
		pipeline.addLast("pipelineExecutor", new ExecutionHandler(
				Configuration.configuration.getHttpPipelineExecutor()));
		pipeline.addLast("streamer", new ChunkedWriteHandler());
		if (useHttpCompression) {
			pipeline.addLast("deflater", new HttpContentCompressor());
		}
		pipeline.addLast("handler", new HttpFormattedHandler());
		return pipeline;
	}
}
