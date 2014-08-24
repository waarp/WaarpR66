/*
 * Copyright 2009 Red Hat, Inc.
 * 
 * Red Hat licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.waarp.openr66.protocol.http.rest.test;

import static io.netty.channel.Channels.*;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelInitializer<SocketChannel>;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.execution.ExecutionHandler;
import io.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.waarp.common.crypto.ssl.WaarpSslContextFactory;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Test Rest client pipeline factory
 */
public class HttpTestRestClientInitializer implements ChannelInitializer<SocketChannel> {
    private final WaarpSslContextFactory waarpSslContextFactory;

    public HttpTestRestClientInitializer(WaarpSslContextFactory waarpSslContextFactory) {
        this.waarpSslContextFactory = waarpSslContextFactory;
    }

    protected void initChannel(Channel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        // Enable HTTPS if necessary.
        if (waarpSslContextFactory != null) {
        	SslHandler handler = waarpSslContextFactory.initInitializer(false,
                    false, false);
        	handler.setIssueHandshake(true);
        	pipeline.addLast("ssl", handler);
        }

        pipeline.addLast("codec", new HttpClientCodec());

        OrderedMemoryAwareThreadPoolExecutor executor = 
                Configuration.configuration.getHttpPipelineExecutor();
        if (executor == null) {
        	Configuration.configuration.httpPipelineInit();
        	executor = 
                    Configuration.configuration.getHttpPipelineExecutor();
        }
        if (executor != null) {
        	pipeline.addLast("pipelineExecutor", new ExecutionHandler(executor));
        }
        // Remove the following line if you don't want automatic content
        // decompression.
        pipeline.addLast("inflater", new HttpContentDecompressor());

        // to be used since huge file transfer
        pipeline.addLast("streamer", new ChunkedWriteHandler());

        pipeline.addLast("handler", new HttpTestResponseHandler());
        return pipeline;
    }
}
