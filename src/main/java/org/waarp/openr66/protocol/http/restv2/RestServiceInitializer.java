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

import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.cdap.http.SSLConfig;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import org.joda.time.DateTime;
import org.waarp.common.crypto.ssl.WaarpSecureKeyStore;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.AbstractRestDbHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.HostConfigHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.HostIdHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.HostsHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.LimitsHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.RuleIdHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.RulesHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.ServerHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.TransferIdHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.TransfersHandler;
import org.waarp.openr66.protocol.http.restv2.resthandlers.RestExceptionHandler;
import org.waarp.openr66.protocol.http.restv2.resthandlers.RestHandlerHook;
import org.waarp.openr66.protocol.http.restv2.resthandlers.RestSignatureHandler;
import org.waarp.openr66.protocol.http.restv2.resthandlers.RestVersionHandler;
import org.waarp.openr66.protocol.networkhandler.ssl.NetworkSslServerInitializer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * This class is called to initialize the RESTv2 API.
 */
public final class RestServiceInitializer {

    /** The logger for all unexpected errors during the service initialization. */
    private static final WaarpLogger logger =
            WaarpLoggerFactory.getLogger(RestServiceInitializer.class);

    /** This is the {@link NettyHttpService} in charge of handling the RESTv2 API. */
    private static NettyHttpService restService;

    /** This is a static class that should never be instantiated with a constructor. */
    private RestServiceInitializer() {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    /** The list of all {@link AbstractRestDbHandler} used by the API. */
    public static final Collection<AbstractRestDbHandler> handlers  =
            new ArrayList<AbstractRestDbHandler>();

    /** Stores the date at which the server was started. */
    public static final DateTime restStartTime = DateTime.now();

    /**
     * Fills the list of {@link AbstractRestDbHandler} with all handlers
     * activated in the API configuration.
     *
     * @param config    The REST API configuration object.
     */
    private static void initHandlers(RestConfiguration config) {
        byte hostsCRUD = config.RESTHANDLERS_CRUD[
                HttpRestR66Handler.RESTHANDLERS.DbHostAuth.ordinal()];
        byte rulesCRUD = config.RESTHANDLERS_CRUD[
                HttpRestR66Handler.RESTHANDLERS.DbRule.ordinal()];
        byte transferCRUD = config.RESTHANDLERS_CRUD[
                HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.ordinal()];
        byte configCRUD = config.RESTHANDLERS_CRUD[
                HttpRestR66Handler.RESTHANDLERS.DbHostConfiguration.ordinal()];
        byte limitCRUD = config.RESTHANDLERS_CRUD[
                HttpRestR66Handler.RESTHANDLERS.Bandwidth.ordinal()];
        int serverCRUD =
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Business.ordinal()] +
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Config.ordinal()] +
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Information.ordinal()] +
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Log.ordinal()] +
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Server.ordinal()] +
                config.RESTHANDLERS_CRUD[
                        HttpRestR66Handler.RESTHANDLERS.Control.ordinal()];

        if (hostsCRUD != 0) {
            handlers.add(new HostsHandler(hostsCRUD));
            handlers.add(new HostIdHandler(hostsCRUD));
        }
        if (rulesCRUD != 0) {
            handlers.add(new RulesHandler(rulesCRUD));
            handlers.add(new RuleIdHandler(rulesCRUD));
        }
        if (transferCRUD != 0) {
            handlers.add(new TransfersHandler(transferCRUD));
            handlers.add(new TransferIdHandler(transferCRUD));
        }
        if (configCRUD != 0) {
            handlers.add(new HostConfigHandler(configCRUD));
        }
        if (limitCRUD != 0) {
            handlers.add(new LimitsHandler(limitCRUD));
        }
        if (serverCRUD != 0) {
            handlers.add(new ServerHandler(config.RESTHANDLERS_CRUD));
        }
    }

    /**
     * Initializes the RESTv2 service with the given {@link RestConfiguration}.
     * @param config    The REST API configuration object.
     */
    public static void initRestService(final RestConfiguration config) {
        initHandlers(config);

        NettyHttpService.Builder restServiceBuilder =
                NettyHttpService.builder("R66_RESTv2")
                .setPort(config.REST_PORT)
                .setHost(config.REST_ADDRESS)
                .setHttpHandlers(handlers)
                .setHandlerHooks(Collections.singleton(new RestHandlerHook(
                        config.REST_AUTHENTICATED, config.hmacSha256,
                        config.REST_TIME_LIMIT)))
                .setExceptionHandler(new RestExceptionHandler())
                .setExecThreadKeepAliveSeconds(-1L)
                .setChannelPipelineModifier(new ChannelPipelineModifier() {
                    @Override
                    public void modify(ChannelPipeline channelPipeline) {
                        channelPipeline.addBefore("router", "aggregator",
                                new HttpObjectAggregator(Integer.MAX_VALUE));
                        channelPipeline.addBefore("router", RestVersionHandler.name,
                                new RestVersionHandler(config));
                        if (config.REST_AUTHENTICATED && config.REST_SIGNATURE) {
                            channelPipeline.addAfter("router", "signature",
                                    new RestSignatureHandler(config.hmacSha256));
                        }

                        //Removes the HTTP compressor which causes problems
                        //on systems running java6 or earlier
                        double JRE_version = Double.parseDouble(
                                System.getProperty("java.specification.version"));
                        if (JRE_version <= 1.6) {
                            logger.info("Removed REST HTTP compressor due to incompatibility " +
                                    "with the Java Runtime version");
                            channelPipeline.remove("compressor");
                        }
                    }
                });

        if (config.REST_SSL) {
            WaarpSecureKeyStore keyStore =
                    NetworkSslServerInitializer.getWaarpSecureKeyStore();
            String keyStoreFilename =
                    new String(keyStore.getKeyStoreFilename());
            String keyStorePass =
                    new String(keyStore.getKeyStorePassword());
            String certificatePassword =
                    new String(keyStore.getCertificatePassword());

            restServiceBuilder.enableSSL(
                    SSLConfig.builder(new File(keyStoreFilename), keyStorePass)
                            .setCertificatePassword(certificatePassword)
                            .build()
            );
        }

        restService = restServiceBuilder.build();
        try {
            restService.start();
        } catch (Throwable t) {
            logger.error(t);
            throw new ExceptionInInitializerError(
                    "FATAL ERROR : Failed to initialize RESTv2 service");
        }
    }


    public static void stopRestService() {
        if (restService != null) {
            try {
                restService.stop();
            } catch (Exception e) {
                logger.error("Exception caught during RESTv2 service shutdown", e);
            }
        } else {
            logger.warn("Error RESTv2 service is not running, cannot stop");
        }
    }
}
