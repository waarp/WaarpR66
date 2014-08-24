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
package org.waarp.openr66.protocol.localhandler;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelInitializer<SocketChannel>;
import io.netty.channel.Channels;
import io.netty.handler.execution.ExecutionHandler;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketCodec;

/**
 * Pipeline Factory for Local Server
 * 
 * @author Frederic Bregier
 */
public class LocalServerInitializer implements ChannelInitializer<SocketChannel> {

	protected void initChannel(Channel ch) throws Exception {
		final ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast("codec", new LocalPacketCodec());
		ExecutionHandler handler = new ExecutionHandler(
				Configuration.configuration.getLocalPipelineExecutor());
		pipeline.addLast("pipelineExecutor", handler);
		pipeline.addLast("handler", new LocalServerHandler());
		return pipeline;
	}

}
