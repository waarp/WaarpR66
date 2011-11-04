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
package openr66.context.task.localexec;

import goldengate.commandexec.client.LocalExecClientHandler;
import goldengate.commandexec.client.LocalExecClientPipelineFactory;
import goldengate.commandexec.utils.LocalExecResult;
import goldengate.common.future.GgFuture;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.InetSocketAddress;

import openr66.protocol.configuration.Configuration;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * Client to execute external command through GoldenGate Local Exec
 *
 * @author Frederic Bregier
 *
 */
public class LocalExecClient {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalExecClient.class);

    static public InetSocketAddress address;
    // Configure the client.
    static private ClientBootstrap bootstrapLocalExec;
    // Configure the pipeline factory.
    static private LocalExecClientPipelineFactory localExecClientPipelineFactory;

    /**
     * Initialize the LocalExec Client context
     */
    public static void initialize() {
        // Configure the client.
        bootstrapLocalExec = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Configuration.configuration.getLocalPipelineExecutor(),
                        Configuration.configuration.getLocalPipelineExecutor()));
        // Configure the pipeline factory.
        localExecClientPipelineFactory =
                new LocalExecClientPipelineFactory();
        bootstrapLocalExec.setPipelineFactory(localExecClientPipelineFactory);
    }

    /**
     * To be called when the server is shutting down to release the resources
     */
    public static void releaseResources() {
        if (bootstrapLocalExec == null) {
            return;
        }
        // Shut down all thread pools to exit.
        bootstrapLocalExec.releaseExternalResources();
        localExecClientPipelineFactory.releaseResources();
    }

    private Channel channel;
    private LocalExecResult result;

    public LocalExecClient() {

    }

    public LocalExecResult getLocalExecResult() {
        return result;
    }

    /**
     * Run one command with a specific allowed delay for execution.
     * The connection must be ready (done with connect()).
     * @param command
     * @param delay
     * @param futureCompletion
     */
    public void runOneCommand(String command, long delay, boolean waitFor, GgFuture futureCompletion) {
     // Initialize the command context
        LocalExecClientHandler clientHandler =
            (LocalExecClientHandler) channel.getPipeline().getLast();
        clientHandler.initExecClient();
        // Command to execute

        ChannelFuture lastWriteFuture = null;
        String line = delay+" "+command+"\n";
        // Sends the received line to the server.
        
        lastWriteFuture = channel.write(line);
        if (!waitFor) {
            futureCompletion.setSuccess();
            logger.info("Exec OK with {}", command);
        }
        // Wait until all messages are flushed before closing the channel.
        if (lastWriteFuture != null) {
            if (delay <= 0) {
                lastWriteFuture.awaitUninterruptibly();
            } else {
                lastWriteFuture.awaitUninterruptibly(delay);
            }
        }
        // Wait for the end of the exec command
        LocalExecResult localExecResult = clientHandler.waitFor(delay*2);
        result = localExecResult;
        if (futureCompletion == null) {
            return;
        }
        if (result.status == 0) {
            if (waitFor) {
                futureCompletion.setSuccess();
            }
            logger.info("Exec OK with {}", command);
        } else if (result.status == 1) {
            logger.warn("Exec in warning with {}", command);
            if (waitFor) {
                futureCompletion.setSuccess();
            }
        } else {
            logger.error("Status: " + result.status + " Exec in error with " +
                    command+"\n"+result.result);
            if (waitFor) {
                futureCompletion.cancel();
            }
        }
    }

    /**
     * Connect to the Server
     */
    public boolean connect() {
        // Start the connection attempt.
        ChannelFuture future = bootstrapLocalExec.connect(address);

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            logger.error("Client Not Connected", future.getCause());
            return false;
        }
        return true;
    }
    /**
     * Disconnect from the server
     */
    public void disconnect() {
     // Close the connection. Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        channel.close().awaitUninterruptibly();
    }
}
