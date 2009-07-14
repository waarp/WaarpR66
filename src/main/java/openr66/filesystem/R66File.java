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
package openr66.filesystem;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.exception.FileEndOfTransferException;
import goldengate.common.exception.FileTransferException;
import goldengate.common.file.DataBlock;
import goldengate.common.file.filesystembased.FilesystemBasedFileImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.exception.OpenR66RunnerErrorException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.DataPacket;
import openr66.protocol.networkhandler.NetworkServerHandler;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.task.TaskRunner;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;

/**
 * @author frederic bregier
 *
 */
public class R66File extends FilesystemBasedFileImpl {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66File.class);

    /**
     * @param session
     * @param dir
     * @param path
     * @param append
     * @throws CommandAbstractException
     */
    public R66File(R66Session session, R66Dir dir, String path, boolean append)
            throws CommandAbstractException {
        super(session, dir, path, append);
    }

    private ChannelFuture writeBack(LocalChannelReference localChannelReference,
            TaskRunner runner, Channel networkChannel, DataBlock block)
            throws OpenR66ProtocolPacketException {
        // FIXME if MD5
        ChannelBuffer md5 = ChannelBuffers.EMPTY_BUFFER;
        DataPacket data = new DataPacket(runner.getRank(), block.getBlock().copy(), md5);
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), data);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Cannot construct message from " + data.toString(),
                    e);
            throw e;
        }
        return Channels.write(networkChannel, networkPacket);
    }
    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.FileInterface#trueRetrieve()
     */
    @Override
    public void trueRetrieve() {
    }
    public void retrieveBlocking() throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        try {
            if (!isReady) {
                return;
            }
            DataBlock block = null;
            try {
                block = readDataBlock();
            } catch (FileEndOfTransferException e) {
                // Last block (in fact, previous block was the last one,
                // but it could be aligned with the block size so not
                // detected)
                this.getSession().setFinalizeTransfer(true, this);
                return;
            }
            if (block == null) {
                // Last block (in fact, previous block was the last one,
                // but it could be aligned with the block size so not
                // detected)
                this.getSession().setFinalizeTransfer(true, this);
                return;
            }
            // While not last block
            LocalChannelReference localChannelReference =
                this.getSession().getLocalChannelReference();
            Channel networkChannel =
                localChannelReference.getNetworkChannel();
            NetworkServerHandler serverHandler =
                localChannelReference.getNetworkServerHandler();
            TaskRunner runner = this.getSession().getRunner();

            ChannelFuture future = null;
            while (block != null && !block.isEOF()) {
                future = writeBack(localChannelReference, runner, networkChannel, block);
                runner.incrementRank();
                // Test if channel is writable in order to prevent OOM
                if (networkChannel.isWritable()) {
                    try {
                        block = readDataBlock();
                    } catch (FileEndOfTransferException e) {
                        // Wait for last write
                        future.awaitUninterruptibly();
                        this.getSession().setFinalizeTransfer(true, this);
                        return;
                    }
                } else {
                    serverHandler.setWriteNotReady();
                    // Wait for the next InterestChanged
                    while (serverHandler.isWriteReady()) {
                        try {
                            Thread.sleep(Configuration.RETRYINMS);
                        } catch (InterruptedException e) {
                            // Wait for last write
                            future.awaitUninterruptibly();
                            this.getSession().setFinalizeTransfer(false, e);
                            return;
                        }
                    }
                    try {
                        block = readDataBlock();
                    } catch (FileEndOfTransferException e) {
                        // Wait for last write
                        future.awaitUninterruptibly();
                        this.getSession().setFinalizeTransfer(true, this);
                        return;
                    }
                }
            }
            // Last block
            if (block != null) {
                future = writeBack(localChannelReference, runner, networkChannel, block);
                runner.incrementRank();
            }
            // Wait for last write
            if (future != null) {
                future.awaitUninterruptibly();
                this.getSession().setFinalizeTransfer(true, this);
                return;
            }
        } catch (FileTransferException e) {
            // An error occurs!
            this.getSession().setFinalizeTransfer(false, e);
        } catch (OpenR66ProtocolPacketException e) {
            // An error occurs!
            this.getSession().setFinalizeTransfer(false, e);
        }
    }

    /**
     * This method is a good to have in a true FileInterface implementation.
     *
     * @return the FileInterface associated with the current FileInterface
     *         operation
     */
    public File getTrueFile() {
        try {
            return getFileFromPath(getFile());
        } catch (CommandAbstractException e) {
            return null;
        }
    }

    public String getBasename() {
        File file = new File(this.currentFile);
        return file.getName();
    }

    @Override
    public R66Session getSession() {
        return (R66Session) this.session;
    }

    public String toString() {
        return "File: "+this.currentFile+" Ready "+this.isReady+" "+this.getPosition();
    }
}
