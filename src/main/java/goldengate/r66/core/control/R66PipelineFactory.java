/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package goldengate.r66.core.control;

import goldengate.common.command.ReplyCode;
import goldengate.r66.core.session.R66Session;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;

/**
 * Pipeline factory for Control command connection
 * 
 * @author Frederic Bregier
 */
public class R66PipelineFactory implements ChannelPipelineFactory {
    /**
     * CRLF, CRNUL, LF delimiters
     */
    private static final ChannelBuffer[] delimiter = new ChannelBuffer[] {
            ChannelBuffers.wrappedBuffer(ReplyCode.CRLF.getBytes()),
            ChannelBuffers.wrappedBuffer(ReplyCode.CRNUL.getBytes()),
            ChannelBuffers.wrappedBuffer(ReplyCode.LF.getBytes()) };

    private static final R66ControlStringDecoder r66ControlStringDecoder = new R66ControlStringDecoder();
    private static final R66ControlStringEncoder r66ControlStringEncoder = new R66ControlStringEncoder();
    /**
     * Business Handler Class if any (Target Mode only)
     */
    private final Class<? extends BusinessHandler> businessHandler;

    /**
     * Configuration
     */
    private final R66Configuration configuration;

    /**
     * Constructor which Initializes some data for Server only
     * 
     * @param businessHandler
     * @param configuration
     */
    public R66PipelineFactory(Class<? extends BusinessHandler> businessHandler,
            R66Configuration configuration) {
        this.businessHandler = businessHandler;
        this.configuration = configuration;
    }

    /**
     * Create the pipeline with Handler, ObjectDecoder, ObjectEncoder.
     * 
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline();
        // Add the text line codec combination first,
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192,
                delimiter));
        pipeline.addLast("decoder", r66ControlStringDecoder);
        pipeline.addLast("encoder", r66ControlStringEncoder);
        // Threaded execution for business logic
        pipeline.addLast("pipelineExecutor", new ExecutionHandler(configuration
                .getR66InternalConfiguration().getPipelineExecutor()));
        // and then business logic. New one on every connection
        final BusinessHandler newbusiness = businessHandler.newInstance();
        final NetworkHandler newNetworkHandler = new NetworkHandler(
                new R66Session(configuration, newbusiness));
        pipeline.addLast("handler", newNetworkHandler);
        return pipeline;
    }
}
