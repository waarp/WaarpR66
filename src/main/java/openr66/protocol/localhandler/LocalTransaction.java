/**
 *
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.StartupPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

/**
 * This class handles Local Transaction connections
 *
 * @author frederic bregier
 */
public class LocalTransaction {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalTransaction.class);
    /**
     * HashMap of LocalChannelReference using LocalChannelId
     */
    final ConcurrentHashMap<Integer, LocalChannelReference> localChannelHashMap = new ConcurrentHashMap<Integer, LocalChannelReference>();
    /**
     * HashMap of LocalChannelReference using requested_requester_specialId
     */
    final ConcurrentHashMap<String, LocalChannelReference> localChannelHashMapExternal =
        new ConcurrentHashMap<String, LocalChannelReference>();
    /**
     * Remover from HashMap
     */
    private final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            LocalChannelReference localChannelReference = localChannelHashMap
                    .remove(future.getChannel().getId());
            if (localChannelReference != null) {
                logger.debug("Remove LocalChannel");
                R66Result result = new R66Result(
                        new OpenR66ProtocolSystemException(
                                "While closing Local Channel"), null, false,
                        ErrorCode.ConnectionImpossible);
                localChannelReference.validateConnection(false, result);
                if (localChannelReference.getSession() != null) {
                    DbTaskRunner runner = localChannelReference.getSession().getRunner();
                    if (runner != null) {
                        String key = runner.getKey();
                        localChannelHashMapExternal.remove(key);
                    }
                }
            }
        }
    };

    private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();

    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;

    private final LocalAddress socketLocalServerAddress = new LocalAddress("0");

    private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup localChannelGroup = new DefaultChannelGroup(
            "LocalChannels");
    /**
     * Constructor
     */
    public LocalTransaction() {
        serverBootstrap.setPipelineFactory(new LocalServerPipelineFactory());
        serverBootstrap.setOption("connectTimeoutMillis",
                Configuration.configuration.TIMEOUTCON);
        serverChannel = serverBootstrap.bind(socketLocalServerAddress);
        localChannelGroup.add(serverChannel);
        clientBootstrap.setPipelineFactory(new LocalClientPipelineFactory());
    }
    /**
     * Get the corresponding LocalChannelReference
     * @param remoteId
     * @param localId
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolSystemException
     */
    public LocalChannelReference getClient(Integer remoteId, Integer localId)
            throws OpenR66ProtocolSystemException {
        LocalChannelReference localChannelReference = getFromId(localId);
        if (localChannelReference != null) {
            if (localChannelReference.getRemoteId() != remoteId) {
                localChannelReference.setRemoteId(remoteId);
            }
            return localChannelReference;
        }
        throw new OpenR66ProtocolSystemException(
                "Cannot find LocalChannelReference");
    }
    /**
     * Create a new Client
     * @param networkChannel
     * @param remoteId
     * @param futureRequest
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolSystemException
     */
    public LocalChannelReference createNewClient(Channel networkChannel,
            Integer remoteId, R66Future futureRequest) throws OpenR66ProtocolSystemException {
        ChannelFuture channelFuture = null;
        logger.debug("Status LocalChannelServer: {} {}",
                serverChannel.getClass().getName(),
                serverChannel.getConfig().getConnectTimeoutMillis() + " " +
                serverChannel.isBound());
        for (int i = 0; i < Configuration.RETRYNB * 2; i ++) {
            channelFuture = clientBootstrap.connect(socketLocalServerAddress);
            channelFuture.awaitUninterruptibly();
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                localChannelGroup.add(channel);
                final LocalChannelReference localChannelReference = new LocalChannelReference(
                            channel, networkChannel, remoteId, futureRequest);
                logger.debug("Create LocalChannel entry: " +i+" {}",
                        localChannelReference);
                localChannelHashMap.put(channel.getId(), localChannelReference);
                channel.getCloseFuture().addListener(remover);
                // Now send first a Startup message
                StartupPacket startup = new StartupPacket(localChannelReference
                        .getLocalId());
                Channels.write(channel, startup).awaitUninterruptibly();
                return localChannelReference;
            } else {
                logger.debug("Can't connect to local server "+i);
            }
            try {
                Thread.sleep(Configuration.RETRYINMS * 2);
            } catch (InterruptedException e) {
                throw new OpenR66ProtocolSystemException(
                        "Cannot connect to local handler", e);
            }
        }
        logger.error("LocalChannelServer: " +
                serverChannel.getClass().getName() + " " +
                serverChannel.getConfig().getConnectTimeoutMillis() + " " +
                serverChannel.isBound());
        throw new OpenR66ProtocolSystemException(
                "Cannot connect to local handler: " + socketLocalServerAddress +
                        " " + serverChannel.isBound() + " " + serverChannel,
                channelFuture.getCause());
    }
    /**
     *
     * @param id
     * @return  the LocalChannelReference
     */
    public LocalChannelReference getFromId(Integer id) {
        return localChannelHashMap.get(id);
    }
    /**
    *
    * @param runner
    * @param lcr
    */
   public void setFromId(DbTaskRunner runner, LocalChannelReference lcr) {
       String key = runner.getKey();
       localChannelHashMapExternal.put(key, lcr);
   }
   /**
   *
   * @param key as "requested requester specialId"
   * @return  the LocalChannelReference
   */
  public LocalChannelReference getFromRequest(String key) {
      return localChannelHashMapExternal.get(key);
  }
  /**
   *
   * @return the number of active local channels
   */
  public int getNumberLocalChannel() {
      return localChannelHashMap.size();
  }
    /**
     * Close all Local Channels from the NetworkChannel
     * @param networkChannel
     */
    public void closeLocalChannelsFromNetworkChannel(Channel networkChannel) {
        Collection<LocalChannelReference> collection = localChannelHashMap
                .values();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        while (iterator.hasNext()) {
            LocalChannelReference localChannelReference = iterator.next();
            if (localChannelReference.getNetworkChannel().compareTo(
                    networkChannel) == 0) {
                logger.debug("Will close local channel");
                Channels.close(localChannelReference.getLocalChannel());
            }
        }
    }
    /**
     * Informs all remote client that the server is shutting down
     */
    public void shutdownLocalChannels() {
        Collection<LocalChannelReference> collection = localChannelHashMap
                .values();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        ValidPacket packet = new ValidPacket("Shutdown forced", null,
                LocalPacketFactory.SHUTDOWNPACKET);
        ChannelBuffer buffer = null;
        while (iterator.hasNext()) {
            LocalChannelReference localChannelReference = iterator.next();
            logger.info("Inform Shutdown {}", localChannelReference);
            packet.setSmiddle(null);
            // If a transfer is running, save the current rank and inform remote
            // host
            if (localChannelReference.getSession() != null) {
                DbTaskRunner runner = localChannelReference.getSession()
                        .getRunner();
                if (runner != null && runner.isInTransfer()) {
                    int rank = localChannelReference.getSession().getRunner()
                            .getRank();
                    packet.setSmiddle(Integer.toString(rank));
                    if (runner.isSender()) {
                        // Save File status
                        try {
                            runner.saveStatus();
                        } catch (OpenR66RunnerErrorException e) {
                        }
                    }
                }
            }
            try {
                buffer = packet.getLocalPacket();
            } catch (OpenR66ProtocolPacketException e1) {
            }
            NetworkPacket message = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), packet
                    .getType(), buffer);
            Channels.write(localChannelReference.getNetworkChannel(), message);
        }
    }
    /**
     * Close All Local Channels
     */
    public void closeAll() {
        logger.warn("close All Local Channels");
        localChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        channelServerFactory.releaseExternalResources();
    }

}
