package org.waarp.openr66.protocol.networkhandler;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Test;

import io.netty.channel.Channel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class NetworkTransactionTest {

    @Test
    public void testisBlacklistedPreventNPE() {
        Channel chan = mock(Channel.class);
        when(chan.remoteAddress()).thenReturn(null);
        NetworkTransaction.isBlacklisted(chan);

        reset(chan);

        InetSocketAddress addr = new InetSocketAddress("cannotberesolved", 6666);
        //assertNull(addr.getAddress());
        doReturn(addr).when(chan).remoteAddress();
        NetworkTransaction.isBlacklisted(chan);
    }
}
