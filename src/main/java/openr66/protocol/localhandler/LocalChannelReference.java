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
package openr66.protocol.localhandler;

import org.jboss.netty.channel.Channel;

/**
 * Reference of one object using Local Channel localId and containing local channel
 * and network channel.
 * 
 * @author Frederic Bregier
 */
public class LocalChannelReference {
    private final Channel localChannel;
    private final Channel networkChannel;
    private final Integer localId;
    private Integer remoteId;

    public LocalChannelReference(Channel localChannel, Channel networkChannel,
            Integer remoteId) {
        this.localChannel = localChannel;
        this.networkChannel = networkChannel;
        localId = this.localChannel.getId();
        this.remoteId = remoteId;
    }

    /**
     * @return the localChannel
     */
    public Channel getLocalChannel() {
        return localChannel;
    }

    /**
     * @return the networkChannel
     */
    public Channel getNetworkChannel() {
        return networkChannel;
    }

    /**
     * @return the id
     */
    public Integer getLocalId() {
        return localId;
    }

    /**
     * @return the remoteId
     */
    public Integer getRemoteId() {
        return remoteId;
    }
    
    /**
     * @param remoteId the remoteId to set
     */
    public void setRemoteId(Integer remoteId) {
        this.remoteId = remoteId;
    }

    @Override
    public String toString() {
        return "LCR: L: "+this.localId+" R: "+this.remoteId;
    }
}
