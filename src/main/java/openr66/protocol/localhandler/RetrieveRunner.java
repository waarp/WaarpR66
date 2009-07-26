/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;

/**
 * @author Frederic Bregier
 *
 */
public class RetrieveRunner implements Runnable {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(RetrieveRunner.class);

    private final R66Session session;

    private final LocalChannelReference localChannelReference;

    private final Channel channel;

    public RetrieveRunner(R66Session session, Channel channel) {
        this.session = session;
        localChannelReference = this.session.getLocalChannelReference();
        this.channel = channel;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName(
                "RetrieveRunner: " + channel.getId());
        try {
            session.getFile().retrieveBlocking();
        } catch (OpenR66RunnerErrorException e) {
            R66Result result = new R66Result(e, session);
            localChannelReference.validateAction(false, result);
            logger.error("Transfer in error", e);
            ErrorPacket error = new ErrorPacket("Transfer in error", e
                    .toString(), ErrorPacket.FORWARDCLOSECODE);
            try {
                writeBack(error);
            } catch (OpenR66ProtocolPacketException e1) {
            }
            ChannelUtils.close(channel);
            logger.warn("End Retrieve in Error");
            return;
        } catch (OpenR66ProtocolSystemException e) {
            R66Result result = new R66Result(e, session);
            localChannelReference.validateAction(false, result);
            logger.error("Transfer in error", e);
            ErrorPacket error = new ErrorPacket("Transfer in error", e
                    .toString(), ErrorPacket.FORWARDCLOSECODE);
            try {
                writeBack(error);
            } catch (OpenR66ProtocolPacketException e1) {
            }
            ChannelUtils.close(channel);
            logger.warn("End Retrieve in Error");
            return;
        }
        logger.info("Await future action to be done");
        localChannelReference.getFutureAction().awaitUninterruptibly();
        if (localChannelReference.getFutureAction().isSuccess()) {
            // send a validation
            ValidPacket validPacket = new ValidPacket("File transmitted",
                    Integer.toString(session.getRunner().getRank()),
                    LocalPacketFactory.REQUESTPACKET);
            try {
                writeBack(validPacket);
            } catch (OpenR66ProtocolPacketException e) {
            }
            ChannelUtils.close(channel);
            return;
        } else {
            ErrorPacket error = new ErrorPacket("Transfer in error",
                    localChannelReference.getFutureAction().getResult()
                            .toString(), ErrorPacket.FORWARDCLOSECODE);
            try {
                writeBack(error);
            } catch (OpenR66ProtocolPacketException e) {
            }
            ChannelUtils.close(channel);
            logger.warn("End Retrieve in Error");
            return;
        }
    }

    private void writeBack(AbstractLocalPacket packet)
            throws OpenR66ProtocolPacketException {
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), packet);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Cannot construct message from " + packet.toString(),
                    e);
            throw e;
        }
        Channels
                .write(localChannelReference.getNetworkChannel(), networkPacket)
                .awaitUninterruptibly();
    }

}
