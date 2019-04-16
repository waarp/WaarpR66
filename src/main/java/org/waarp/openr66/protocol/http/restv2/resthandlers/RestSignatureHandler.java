/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
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

package org.waarp.openr66.protocol.http.restv2.resthandlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import org.waarp.common.crypto.HmacSha256;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.AbstractRestDbHandler;
import org.waarp.openr66.protocol.http.restv2.utils.RestUtils;

import javax.ws.rs.InternalServerErrorException;
import java.util.Locale;

import static io.netty.channel.ChannelFutureListener.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static javax.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.AUTH_SIGNATURE;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.UTF8_CHARSET;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.REQUEST_NOT_SIGNED;

/**
 * When a request is made to the REST API, and if the request signature checking
 * is activated in the API parameters, then this handler is called after
 * the {@link RestHandlerHook} and before the corresponding
 * {@link AbstractRestDbHandler}
 * to check the validity of the request's signature.
 */
public class RestSignatureHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    /** The logger for all unexpected errors during the execution. */
    private final WaarpLogger logger =
            WaarpLoggerFactory.getLogger(this.getClass());

    /** The HMAC key used to create the request's signature. */
    private final HmacSha256 hmac;

    /**
     * Initializes the handler with the given HMAC key.
     * @param hmac The REST HMAC signing key.
     */
    public RestSignatureHandler(HmacSha256 hmac) {
        this.hmac = hmac;
    }

    /**
     * Sends a reply to the request stating that the request was not properly
     * signed.
     *
     * @param ctx       The context of the Netty channel handler.
     * @param request   The original HTTP request.
     */
    private void unsigned(ChannelHandlerContext ctx, FullHttpRequest request) {
        FullHttpResponse response;
        try {
            Locale lang = RestUtils.getRequestLocale(request);
            byte[] body = REQUEST_NOT_SIGNED().serialize(lang).getBytes();
            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add(CONTENT_TYPE, APPLICATION_JSON);
            headers.add(CONTENT_LENGTH, body.length);
            ByteBuf content = Unpooled.wrappedBuffer(body);

            response = new DefaultFullHttpResponse(HTTP_1_1,
                    BAD_REQUEST, content, headers, EmptyHttpHeaders.INSTANCE);

            logger.info("Unsigned request received.");
        } catch (InternalServerErrorException e) {
            logger.error(e);
            response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
        }
        ctx.channel().writeAndFlush(response).addListener(CLOSE);
    }

    /**
     * Checks if the request given as parameter by the channel pipeline is
     * properly signed or not. If the signature is valid, the request is
     * forwarded to the corresponding {@link AbstractRestDbHandler}, otherwise
     * a reply is directly sent stating that the request needs to be signed.
     * If an unexpected error occurs during the execution, an error 500 HTTP
     * status is sent instead.
     *
     * @param ctx       The context of the Netty channel handler.
     * @param request   The original HTTP request.
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {

        // If the request does not have a body, skip the signature checking.
        if (!request.content().isReadable()) {
            ctx.fireChannelRead(request.retain());
            return;
        }

        String body = request.content().toString(UTF8_CHARSET);
        String URI = request.uri();
        String method = request.method().toString();
        String sign = request.headers().get(AUTH_SIGNATURE);

        FullHttpResponse response;

        if (sign == null) {
            unsigned(ctx, request);
            return;
        }

        String computedHash;
        try {
            computedHash = this.hmac.cryptToHex(body + URI + method);
        } catch (Exception e) {
            logger.error(e);
            response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
            ctx.channel().writeAndFlush(response).addListener(CLOSE);
            return;
        }

        if (!computedHash.equals(sign)) {
            unsigned(ctx, request);
            return;
        }

        ctx.fireChannelRead(request.retain());
    }

}
