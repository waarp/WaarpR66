

package org.waarp.openr66.protocol.http.restv2.resthandlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;

/**
 * Handles the dispatching of incoming request between version 1 and 2 of the
 * REST API. By default, the pipeline continues with the v2 handler, but if the
 * request URI does not match the pattern of a v2 entry point, then the pipeline
 * will switch to the v1 handler.
 */
public class RestVersionHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final WaarpLogger logger =
            WaarpLoggerFactory.getLogger(RestVersionHandler.class);

    /** Name of this handler in the Netty pipeline. */
    public final static String name = "version_handler";

    /** Name of the RESTv1 handler in the Netty pipeline. */
    private final static String handlerName = "v1_handler";

    /** The RESTv1 handler. */
    private final HttpRestR66Handler restV1Handler;

    /**
     * Initializes the REST version splitter and handler with the given REST
     * configuration.
     *
     * @param restConfiguration The {@link RestConfiguration} object.
     */
    public RestVersionHandler(RestConfiguration restConfiguration) {
        super(false);
        HttpRestR66Handler.instantiateHandlers(restConfiguration);
        restV1Handler = new HttpRestR66Handler(restConfiguration);
    }

    /**
     * Dispatches the incoming request to the corresponding v1 or v2 REST handler.
     * @param ctx       The Netty pipeline context.
     * @param request   The incoming request.
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        logger.debug(request.method() + " received on " + request.uri());

        if (request.uri().startsWith("/v2/")) {
            if (ctx.pipeline().get(handlerName) != null) {
                ctx.pipeline().remove(restV1Handler.getClass());
            }
            ctx.fireChannelRead(request);
        } else {
            if (ctx.pipeline().get(handlerName) == null) {
                ctx.pipeline().addAfter(name, handlerName, restV1Handler);
            }
            ctx.fireChannelRead(request);
        }
    }
}
