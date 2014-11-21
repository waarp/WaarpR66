/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.traffic;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This implementation of the {@link AbstractTrafficShapingHandler} is for global
 * and per channel traffic shaping, that is to say a global limitation of the bandwidth, whatever
 * the number of opened channels and a per channel limitation of the bandwidth.<br><br>
 * This version shall not be in the same pipeline than other TrafficShapingHandler.<br><br>
 *
 * The general use should be as follow:<br>
 * <ul>
 * <li>Create your unique GlobalChannelTrafficShapingHandler like:<br><br>
 * <tt>GlobalChannelTrafficShapingHandler myHandler = new GlobalChannelTrafficShapingHandler(executor);</tt><br><br>
 * The executor could be the underlying IO worker pool<br>
 * <tt>pipeline.addLast(myHandler);</tt><br><br>
 *
 * <b>Note that this handler has a Pipeline Coverage of "all" which means only one such handler must be created
 * and shared among all channels as the counter must be shared among all channels.</b><br><br>
 *
 * Other arguments can be passed like write or read limitation (in bytes/s where 0 means no limitation)
 * or the check interval (in millisecond) that represents the delay between two computations of the
 * bandwidth and so the call back of the doAccounting method (0 means no accounting at all).<br>
 * Note that as this is a fusion of both Global and Channel Traffic Shaping, limits are in 2 sets,
 * respectively Global and Channel.<br><br>
 *
 * A value of 0 means no accounting for checkInterval. If you need traffic shaping but no such accounting,
 * it is recommended to set a positive value, even if it is high since the precision of the
 * Traffic Shaping depends on the period where the traffic is computed. The highest the interval,
 * the less precise the traffic shaping will be. It is suggested as higher value something close
 * to 5 or 10 minutes.<br><br>
 *
 * maxTimeToWait, by default set to 15s, allows to specify an upper bound of time shaping.<br><br>
 * </li>
 * <li>In your handler, you should consider to use the <code>channel.isWritable()</code> and
 * <code>channelWritabilityChanged(ctx)</code> to handle writability, or through
 * <code>future.addListener(new GenericFutureListener())</code> on the future returned by
 * <code>ctx.write()</code>.</li>
 * <li>You shall also consider to have object size in read or write operations relatively adapted to
 * the bandwidth you required: for instance having 10 MB objects for 10KB/s will lead to burst effect,
 * while having 100 KB objects for 1 MB/s should be smoothly handle by this TrafficShaping handler.<br><br></li>
 * <li>Some configuration methods will be taken as best effort, meaning
 * that all already scheduled traffics will not be
 * changed, but only applied to new traffics.<br>
 * So the expected usage of those methods are to be used not too often,
 * accordingly to the traffic shaping configuration.</li>
 * </ul><br>
 *
 * Be sure to call {@link #release()} once this handler is not needed anymore to release all internal resources.
 * This will not shutdown the {@link EventExecutor} as it may be shared, so you need to do this by your own.
 */
@Sharable
public class GlobalChannelTrafficShapingHandler extends AbstractTrafficShapingHandler {
    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(GlobalChannelTrafficShapingHandler.class);
    /**
     * All queues per channel
     */
    final ConcurrentMap<Integer, MixtePerChannel> channelQueues = PlatformDependent.newConcurrentHashMap();

    /**
     * Global queues size
     */
    private final AtomicLong queuesSize = new AtomicLong();

    /**
     * Maximum cumulative writing bytes for one channel among all (as long as channels stay the same)
     */
    private final AtomicLong cumulativeWrittenBytes = new AtomicLong();

    /**
     * Maximum cumulative read bytes for one channel among all (as long as channels stay the same)
     */
    private final AtomicLong cumulativeReadBytes = new AtomicLong();

    /**
     * Max size in the list before proposing to stop writing new objects from next handlers
     * for all channel (global)
     */
    long maxGlobalWriteSize = DEFAULT_MAX_SIZE * 100; // default 400MB

    /**
     * Limit in B/s to apply to write
     */
    private long writeChannelLimit;

    /**
     * Limit in B/s to apply to read
     */
    private long readChannelLimit;

    private static final float DEFAULT_DEVIATION = 0.1F;
    private float maxDeviation;
    private float minValueDeviation;
    private float maxValueDeviation;
    private volatile boolean readDeviationActive;
    private volatile boolean writeDeviationActive;

    static class MixtePerChannel {
        ArrayDeque<ToSend> messagesQueue;
        TrafficCounter channelTrafficCounter;
        ReentrantLock lock;
        long queueSize;
        long lastWrite;
        long lastRead;
    }

    /**
     * Create the global TrafficCounter
     */
    void createGlobalTrafficCounter(ScheduledExecutorService executor) {
        // Default
        setMaxDeviation(DEFAULT_DEVIATION);
        if (executor == null) {
            throw new NullPointerException("executor");
        }
        TrafficCounter tc = new GlobalChannelTrafficCounter(this, executor, "GlobalTC", checkInterval);
        setTrafficCounter(tc);
        tc.start();
    }

    @Override
    int userDefinedWritabilityIndex() {
        return AbstractTrafficShapingHandler.GLOBAL_DEFAULT_USER_DEFINED_WRITABILITY_INDEX;
    }

    /**
     * Create a new instance
     *
     * @param executor
     *            the {@link ScheduledExecutorService} to use for the {@link TrafficCounter}
     * @param writeGlobalLimit
     *            0 or a limit in bytes/s
     * @param readGlobalLimit
     *            0 or a limit in bytes/s
     * @param writeChannelLimit
     *            0 or a limit in bytes/s
     * @param readChannelLimit
     *            0 or a limit in bytes/s
     * @param checkInterval
     *            The delay between two computations of performances for
     *            channels or 0 if no stats are to be computed
     * @param maxTime
     *            The maximum delay to wait in case of traffic excess
     */
    public GlobalChannelTrafficShapingHandler(ScheduledExecutorService executor,
            long writeGlobalLimit, long readGlobalLimit,
            long writeChannelLimit, long readChannelLimit,
            long checkInterval, long maxTime) {
        super(writeGlobalLimit, readGlobalLimit, checkInterval, maxTime);
        createGlobalTrafficCounter(executor);
        this.writeChannelLimit = writeChannelLimit;
        this.readChannelLimit = readChannelLimit;
    }

    /**
     * Create a new instance
     *
     * @param executor
     *          the {@link ScheduledExecutorService} to use for the {@link TrafficCounter}
     * @param writeGlobalLimit
     *            0 or a limit in bytes/s
     * @param readGlobalLimit
     *            0 or a limit in bytes/s
     * @param writeChannelLimit
     *            0 or a limit in bytes/s
     * @param readChannelLimit
     *            0 or a limit in bytes/s
     * @param checkInterval
     *          The delay between two computations of performances for
     *            channels or 0 if no stats are to be computed
     */
    public GlobalChannelTrafficShapingHandler(ScheduledExecutorService executor,
            long writeGlobalLimit, long readGlobalLimit,
            long writeChannelLimit, long readChannelLimit,
            long checkInterval) {
        super(writeGlobalLimit, readGlobalLimit, checkInterval);
        this.writeChannelLimit = writeChannelLimit;
        this.readChannelLimit = readChannelLimit;
        createGlobalTrafficCounter(executor);
    }

    /**
     * Create a new instance
     *
     * @param executor
     *          the {@link ScheduledExecutorService} to use for the {@link TrafficCounter}
     * @param writeGlobalLimit
     *            0 or a limit in bytes/s
     * @param readGlobalLimit
     *            0 or a limit in bytes/s
     * @param writeChannelLimit
     *            0 or a limit in bytes/s
     * @param readChannelLimit
     *            0 or a limit in bytes/s
     */
    public GlobalChannelTrafficShapingHandler(ScheduledExecutorService executor,
            long writeGlobalLimit, long readGlobalLimit,
            long writeChannelLimit, long readChannelLimit) {
        super(writeGlobalLimit, readGlobalLimit);
        this.writeChannelLimit = writeChannelLimit;
        this.readChannelLimit = readChannelLimit;
        createGlobalTrafficCounter(executor);
    }

    /**
     * Create a new instance
     *
     * @param executor
     *          the {@link ScheduledExecutorService} to use for the {@link TrafficCounter}
     * @param checkInterval
     *          The delay between two computations of performances for
     *            channels or 0 if no stats are to be computed
     */
    public GlobalChannelTrafficShapingHandler(ScheduledExecutorService executor, long checkInterval) {
        super(checkInterval);
        createGlobalTrafficCounter(executor);
    }

    /**
     * Create a new instance
     *
     * @param executor
     *          the {@link ScheduledExecutorService} to use for the {@link TrafficCounter}
     */
    public GlobalChannelTrafficShapingHandler(ScheduledExecutorService executor) {
        createGlobalTrafficCounter(executor);
    }

    /**
     * @return the current max deviation
     */
    public float maxDeviation() {
        return maxDeviation;
    }

    /**
     * @param maxDeviation the maximum deviation to allow during computation of average,
     *      default deviation being 0.1, so +/-10% of the desired bandwidth. Maximum being 0.4.
     */
    public void setMaxDeviation(float maxDeviation) {
        if (maxDeviation > 0.4F) {
            // Ignore
            return;
        }
        this.maxDeviation = maxDeviation;
        minValueDeviation = 1.0F - maxDeviation;
        // Penalize more too fast channels
        maxValueDeviation = 1.0F + maxDeviation * 4;
    }

    private void computeDeviationCumulativeBytes() {
        // compute the maximum cumulativeXxxxBytes among still connected Channels
        long maxWrittenBytes = 0;
        long maxReadBytes = 0;
        long minWrittenBytes = Long.MAX_VALUE;
        long minReadBytes = Long.MAX_VALUE;
        for (MixtePerChannel mixtePerChannel : channelQueues.values()) {
            long value = mixtePerChannel.channelTrafficCounter.cumulativeWrittenBytes();
            if (maxWrittenBytes < value) {
                maxWrittenBytes = value;
            }
            if (minWrittenBytes > value) {
                minWrittenBytes = value;
            }
            value = mixtePerChannel.channelTrafficCounter.cumulativeReadBytes();
            if (maxReadBytes < value) {
                maxReadBytes = value;
            }
            if (minReadBytes > value) {
                minReadBytes = value;
            }
        }
        boolean multiple = channelQueues.size() > 1;
        readDeviationActive = multiple && minReadBytes < maxReadBytes / 2;
        writeDeviationActive = multiple && minWrittenBytes < maxWrittenBytes / 2;
        cumulativeWrittenBytes.set(maxWrittenBytes);
        cumulativeReadBytes.set(maxReadBytes);
    }

    @Override
    protected void doAccounting(TrafficCounter counter) {
        computeDeviationCumulativeBytes();
        super.doAccounting(counter);
    }

    private long computeBalancedWait(float maxLocal, float maxGlobal, long wait) {
        float ratio = maxLocal / maxGlobal;
        // if in the boundaries, same value
        if (ratio > maxDeviation) {
            if (ratio < minValueDeviation) {
                return wait;
            } else {
                ratio = maxValueDeviation;
                if (wait < MINIMAL_WAIT) {
                    wait = MINIMAL_WAIT;
                }
            }
        } else {
            ratio = minValueDeviation;
        }
        return (long) (wait * ratio);
    }

    /**
     * @return the maxGlobalWriteSize
     */
    public long getMaxGlobalWriteSize() {
        return maxGlobalWriteSize;
    }

    /**
     * Note the change will be taken as best effort, meaning
     * that all already scheduled traffics will not be
     * changed, but only applied to new traffics.<br>
     * So the expected usage of this method is to be used not too often,
     * accordingly to the traffic shaping configuration.
     *
     * @param maxGlobalWriteSize the maximum Global Write Size allowed in the buffer
     *            globally for all channels before write suspended is set
     */
    public void setMaxGlobalWriteSize(long maxGlobalWriteSize) {
        this.maxGlobalWriteSize = maxGlobalWriteSize;
    }

    /**
     * @return the global size of the buffers for all queues
     */
    public long queuesSize() {
        return queuesSize.get();
    }

    /**
     * @param newWriteLimit Channel write limit
     * @param newReadLimit Channel read limit
     */
    public void configureChannel(long newWriteLimit, long newReadLimit) {
        writeChannelLimit = newWriteLimit;
        readChannelLimit = newReadLimit;
        long now = TrafficCounter.milliSecondFromNano() + 1;
        for (MixtePerChannel mixtePerChannel : channelQueues.values()) {
            mixtePerChannel.channelTrafficCounter.resetAccounting(now);
        }
    }

    /**
     * @return Channel write limit
     */
    public long getWriteChannelLimit() {
        return writeChannelLimit;
    }

    /**
     * @param Channel write limit
     */
    public void setWriteChannelLimit(long writeLimit) {
        writeChannelLimit = writeLimit;
        long now = TrafficCounter.milliSecondFromNano() + 1;
        for (MixtePerChannel mixtePerChannel : channelQueues.values()) {
            mixtePerChannel.channelTrafficCounter.resetAccounting(now);
        }
    }

    /**
     * @return Channel read limit
     */
    public long getReadChannelLimit() {
        return readChannelLimit;
    }

    /**
     * @return Channel read limit
     */
    public void setReadChannelLimit(long readLimit) {
        readChannelLimit = readLimit;
        long now = TrafficCounter.milliSecondFromNano() + 1;
        for (MixtePerChannel mixtePerChannel : channelQueues.values()) {
            mixtePerChannel.channelTrafficCounter.resetAccounting(now);
        }
    }

    /**
     * Release all internal resources of this instance
     */
    public final void release() {
        trafficCounter.stop();
    }

    private MixtePerChannel getOrSetPerChannel(ChannelHandlerContext ctx) {
        // ensure creation is limited to one thread per channel
        Channel channel = ctx.channel();
        Integer key = channel.hashCode();
        MixtePerChannel mixtePerChannel = channelQueues.get(key);
        if (mixtePerChannel == null) {
            mixtePerChannel = new MixtePerChannel();
            mixtePerChannel.messagesQueue = new ArrayDeque<ToSend>();
            // Don't start it since managed through the Global one
            mixtePerChannel.channelTrafficCounter = new TrafficCounter(this, null, "ChannelTC" +
                    ctx.channel().hashCode(), checkInterval);
            mixtePerChannel.lock = new ReentrantLock(true);
            mixtePerChannel.queueSize = 0L;
            mixtePerChannel.lastRead = TrafficCounter.milliSecondFromNano();
            mixtePerChannel.lastWrite = mixtePerChannel.lastRead;
            channelQueues.put(key, mixtePerChannel);
        }
        return mixtePerChannel;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        getOrSetPerChannel(ctx);
        trafficCounter.resetCumulativeTime();
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        trafficCounter.resetCumulativeTime();
        Channel channel = ctx.channel();
        Integer key = channel.hashCode();
        MixtePerChannel mixtePerChannel = channelQueues.remove(key);
        if (mixtePerChannel != null) {
            // write operations need synchronization
            mixtePerChannel.lock.lock();
            try {
                if (channel.isActive()) {
                    for (ToSend toSend : mixtePerChannel.messagesQueue) {
                        long size = calculateSize(toSend.toSend);
                        trafficCounter.bytesRealWriteFlowControl(size);
                        mixtePerChannel.channelTrafficCounter.bytesRealWriteFlowControl(size);
                        mixtePerChannel.queueSize -= size;
                        queuesSize.addAndGet(-size);
                        ctx.write(toSend.toSend, toSend.promise);
                    }
                } else {
                    queuesSize.addAndGet(-mixtePerChannel.queueSize);
                    for (ToSend toSend : mixtePerChannel.messagesQueue) {
                        if (toSend.toSend instanceof ByteBuf) {
                            ((ByteBuf) toSend.toSend).release();
                        }
                    }
                }
                mixtePerChannel.messagesQueue.clear();
            } finally {
                mixtePerChannel.lock.unlock();
            }
        }
        releaseWriteSuspended(ctx);
        releaseReadSuspended(ctx);
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        long size = calculateSize(msg);
        long now = TrafficCounter.milliSecondFromNano();
        if (size > 0) {
            // compute the number of ms to wait before reopening the channel
            long waitGlobal = trafficCounter.readTimeToWait(size, getReadLimit(), maxTime, now);
            Integer key = ctx.channel().hashCode();
            MixtePerChannel mixtePerChannel = channelQueues.get(key);
            long wait = 0;
            if (mixtePerChannel != null) {
                wait = mixtePerChannel.channelTrafficCounter.readTimeToWait(size, readChannelLimit, maxTime, now);
                if (readDeviationActive) {
                    // now try to balance between the channels
                    long maxLocalRead = 0;
                    maxLocalRead = mixtePerChannel.channelTrafficCounter.cumulativeReadBytes();
                    long maxGlobalRead = cumulativeReadBytes.get();
                    if (maxLocalRead <= 0) {
                        maxLocalRead = 1;
                    }
                    if (maxGlobalRead < maxLocalRead) {
                        maxGlobalRead = maxLocalRead;
                    }
                    wait = computeBalancedWait(maxLocalRead, maxGlobalRead, wait);
                }
            }
            if (wait < waitGlobal) {
                wait = waitGlobal;
            }
            wait = checkWaitReadTime(ctx, wait, now);
            if (wait >= MINIMAL_WAIT) { // At least 10ms seems a minimal
                // time in order to try to limit the traffic
                // Only AutoRead AND HandlerActive True means Context Active
                ChannelConfig config = ctx.channel().config();
                if (logger.isDebugEnabled()) {
                    logger.debug("Read Suspend: " + wait + ":" + config.isAutoRead() + ":"
                            + isHandlerActive(ctx));
                }
                if (config.isAutoRead() && isHandlerActive(ctx)) {
                    config.setAutoRead(false);
                    ctx.attr(READ_SUSPENDED).set(true);
                    // Create a Runnable to reactive the read if needed. If one was create before it will just be
                    // reused to limit object creation
                    Attribute<Runnable> attr = ctx.attr(REOPEN_TASK);
                    Runnable reopenTask = attr.get();
                    if (reopenTask == null) {
                        reopenTask = new ReopenReadTimerTask(ctx);
                        attr.set(reopenTask);
                    }
                    ctx.executor().schedule(reopenTask, wait, TimeUnit.MILLISECONDS);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Suspend final status => " + config.isAutoRead() + ":"
                                + isHandlerActive(ctx) + " will reopened at: " + wait);
                    }
                }
            }
        }
        informReadOperation(ctx, now);
        ctx.fireChannelRead(msg);
    }

    @Override
    protected long checkWaitReadTime(final ChannelHandlerContext ctx, long wait, final long now) {
        Integer key = ctx.channel().hashCode();
        MixtePerChannel mixtePerChannel = channelQueues.get(key);
        if (mixtePerChannel != null) {
            if (wait > maxTime && now + wait - mixtePerChannel.lastRead > maxTime) {
                wait = maxTime;
            }
        }
        return wait;
    }

    @Override
    protected void informReadOperation(final ChannelHandlerContext ctx, final long now) {
        Integer key = ctx.channel().hashCode();
        MixtePerChannel mixtePerChannel = channelQueues.get(key);
        if (mixtePerChannel != null) {
            mixtePerChannel.lastRead = now;
        }
    }

    private static final class ToSend {
        final long relativeTimeAction;
        final Object toSend;
        final ChannelPromise promise;
        final long size;

        private ToSend(final long delay, final Object toSend, final long size, final ChannelPromise promise) {
            this.relativeTimeAction = delay;
            this.toSend = toSend;
            this.size = size;
            this.promise = promise;
        }
    }

    protected long maximumCumulativeWrittenBytes() {
        return cumulativeWrittenBytes.get();
    }

    protected long maximumCumulativeReadBytes() {
        return cumulativeReadBytes.get();
    }

    protected Collection<TrafficCounter> channelTrafficCounters() {
        List<TrafficCounter> list = new ArrayList<TrafficCounter>(channelQueues.size());
        for (MixtePerChannel mpc : channelQueues.values()) {
            list.add(mpc.channelTrafficCounter);
        }
        return list;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
            throws Exception {
        long size = calculateSize(msg);
        long now = TrafficCounter.milliSecondFromNano();
        if (size > 0) {
            // compute the number of ms to wait before continue with the channel
            long waitGlobal = trafficCounter.writeTimeToWait(size, getWriteLimit(), maxTime, now);
            Integer key = ctx.channel().hashCode();
            MixtePerChannel mixtePerChannel = channelQueues.get(key);
            long wait = 0;
            if (mixtePerChannel != null) {
                wait = mixtePerChannel.channelTrafficCounter.writeTimeToWait(size, writeChannelLimit, maxTime, now);
                if (writeDeviationActive) {
                    // now try to balance between the channels
                    long maxLocalWrite = 0;
                    maxLocalWrite = mixtePerChannel.channelTrafficCounter.cumulativeWrittenBytes();
                    long maxGlobalWrite = cumulativeWrittenBytes.get();
                    if (maxLocalWrite <= 0) {
                        maxLocalWrite = 1;
                    }
                    if (maxGlobalWrite < maxLocalWrite) {
                        maxGlobalWrite = maxLocalWrite;
                    }
                    wait = computeBalancedWait(maxLocalWrite, maxGlobalWrite, wait);
                }
            }
            if (wait < waitGlobal) {
                wait = waitGlobal;
            }
            if (wait >= MINIMAL_WAIT) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Write suspend: " + wait + ":" + ctx.channel().config().isAutoRead() + ":"
                            + isHandlerActive(ctx));
                }
                submitWrite(ctx, msg, size, wait, now, promise);
                return;
            }
        }
        // to maintain order of write
        submitWrite(ctx, msg, size, 0, now, promise);
    }

    @Override
    protected void submitWrite(final ChannelHandlerContext ctx, final Object msg,
            final long size, final long writedelay, final long now,
            final ChannelPromise promise) {
        Channel channel = ctx.channel();
        Integer key = channel.hashCode();
        MixtePerChannel mixtePerChannel = channelQueues.get(key);
        if (mixtePerChannel == null) {
            // in case write occurs before handlerAdded is raized for this handler
            // imply a synchronized only if needed
            mixtePerChannel = getOrSetPerChannel(ctx);
        }
        ToSend newToSend;
        long delay = writedelay;
        boolean globalSizeExceeded = false;
        // write operations need synchronization
        mixtePerChannel.lock.lock();
        try {
            if (writedelay == 0 && mixtePerChannel.messagesQueue.isEmpty()) {
                trafficCounter.bytesRealWriteFlowControl(size);
                mixtePerChannel.channelTrafficCounter.bytesRealWriteFlowControl(size);
                ctx.writeAndFlush(msg, promise);
                mixtePerChannel.lastWrite = now;
                return;
            }
            if (delay > maxTime && now + delay - mixtePerChannel.lastWrite > maxTime) {
                delay = maxTime;
            }
            newToSend = new ToSend(delay + now, msg, size, promise);
            mixtePerChannel.messagesQueue.addLast(newToSend);
            mixtePerChannel.queueSize += size;
            queuesSize.addAndGet(size);
            checkWriteSuspend(ctx, delay, mixtePerChannel.queueSize);
            if (queuesSize.get() > maxGlobalWriteSize) {
                globalSizeExceeded = true;
            }
        } finally {
            mixtePerChannel.lock.unlock();
        }
        if (globalSizeExceeded) {
            ChannelOutboundBuffer cob = channel.unsafe().outboundBuffer();
            if (cob != null) {
                cob.setUserDefinedWritability(userDefinedWritabilityIndex, false);
            }
        }
        final long futureNow = newToSend.relativeTimeAction;
        final MixtePerChannel forSchedule = mixtePerChannel;
        ctx.executor().schedule(new Runnable() {
            @Override
            public void run() {
                sendAllValid(ctx, forSchedule, futureNow);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void sendAllValid(final ChannelHandlerContext ctx, final MixtePerChannel mixtePerChannel, final long now) {
        // write operations need synchronization
        mixtePerChannel.lock.lock();
        try {
            ToSend newToSend = mixtePerChannel.messagesQueue.pollFirst();
            for (; newToSend != null; newToSend = mixtePerChannel.messagesQueue.pollFirst()) {
                if (newToSend.relativeTimeAction <= now) {
                    long size = newToSend.size;
                    trafficCounter.bytesRealWriteFlowControl(size);
                    mixtePerChannel.channelTrafficCounter.bytesRealWriteFlowControl(size);
                    mixtePerChannel.queueSize -= size;
                    queuesSize.addAndGet(-size);
                    ctx.write(newToSend.toSend, newToSend.promise);
                    mixtePerChannel.lastWrite = now;
                } else {
                    mixtePerChannel.messagesQueue.addFirst(newToSend);
                    break;
                }
            }
            if (mixtePerChannel.messagesQueue.isEmpty()) {
                releaseWriteSuspended(ctx);
            }
        } finally {
            mixtePerChannel.lock.unlock();
        }
        ctx.flush();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString());
        builder.append(" Write Channel Limit: ").append(writeChannelLimit);
        builder.append(" Read Channel Limit: ").append(readChannelLimit);
        return builder.toString();
    }
}
