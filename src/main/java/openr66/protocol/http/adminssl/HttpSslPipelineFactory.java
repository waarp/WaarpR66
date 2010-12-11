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
package openr66.protocol.http.adminssl;

import goldengate.common.crypto.ssl.GgSecureKeyStore;
import goldengate.common.crypto.ssl.GgSslContextFactory;

import java.util.concurrent.ExecutorService;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;

/**
 * @author Frederic Bregier
 *
 */
public class HttpSslPipelineFactory implements ChannelPipelineFactory {
    public static GgSslContextFactory ggSslContextFactory;
    public static GgSecureKeyStore ggSecureKeyStore;
    private final ExecutorService executorService;
    public boolean useHttpCompression = false;
    public boolean enableRenegotiation = false;

    public HttpSslPipelineFactory(boolean useHttpCompression,
            boolean enableRenegotiation,
            ExecutorService executor) {
        this.useHttpCompression = useHttpCompression;
        this.enableRenegotiation = enableRenegotiation;
        this.executorService = executor;
    }

    @Override
    public ChannelPipeline getPipeline() {
        final ChannelPipeline pipeline = Channels.pipeline();
        // Add SSL handler first to encrypt and decrypt everything.
        pipeline.addLast("ssl",
                ggSslContextFactory.initPipelineFactory(true,
                        false,
                        enableRenegotiation, executorService));

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                Configuration.configuration.getHttpPipelineExecutor()));
        // FIXME: make an option for compression on HTTP
        if (useHttpCompression) {
            pipeline.addLast("deflater", new HttpContentCompressor());
        }
        pipeline.addLast("handler", new HttpSslHandler());
        return pipeline;
    }
}
