/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.http.adminssl;

import static org.jboss.netty.channel.Channels.pipeline;

import javax.net.ssl.SSLEngine;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http2.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http2.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http2.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * Pipeline Factory for HTTP support
 * @author Frederic Bregier
 *
 */
public class HttpSslPipelineFactory
    implements ChannelPipelineFactory {

    public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = pipeline();

         // Add SSL handler first to encrypt and decrypt everything.
            // You will need something more complicated to identify both
            // and server in the real world.
            SSLEngine engine;
            SslHandler sslhandler;
            engine = HttpSecureSslContextFactory.getServerContext()
                .createSSLEngine();
            engine.setUseClientMode(false);
            engine.setNeedClientAuth(false);
            sslhandler = new SslHandler(engine);
            pipeline.addLast("ssl", sslhandler);

            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
            pipeline.addLast("encoder", new HttpResponseEncoder());
            pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                    Configuration.configuration.getHttpPipelineExecutor()));
            pipeline.addLast("handler", new HttpSslHandler());
            return pipeline;
        }
}
