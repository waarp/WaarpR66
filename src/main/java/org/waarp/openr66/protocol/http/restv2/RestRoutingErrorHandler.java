/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.waarp.openr66.protocol.http.restv2;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.waarp.openr66.protocol.http.restv2.errors.RestResponse;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** An element of the network pipeline situated just under the request router. Handles error sent by the router. */
public class RestRoutingErrorHandler extends ChannelOutboundHandlerAdapter {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * @param ctx     The pipeline channel context to which the response is written.
     * @param msg     The http response created by the upper members of the pipeline.
     * @param promise The channel promise of the pipeline.
     * @throws Exception Thrown if an error occurs during the response processing, or if an unknown exception was
     *                   thrown by a upper member of the pipeline.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof FullHttpResponse)) {
            super.write(ctx, msg, promise);
            return;
        }

        FullHttpResponse fullResponse;
        fullResponse = ((FullHttpResponse) msg).copy();

        FullHttpResponse newResponse;
        switch (fullResponse.status().code()) {
            case 404:
            case 405:
                fullResponse.headers().remove(HttpHeaderNames.CONTENT_TYPE);
                newResponse = fullResponse.replace(Unpooled.copiedBuffer("", UTF_8));
                newResponse.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
                newResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, 0);
                super.write(ctx, newResponse, promise);
                break;
            default:
                super.write(ctx, msg, promise);

        }
    }
}
