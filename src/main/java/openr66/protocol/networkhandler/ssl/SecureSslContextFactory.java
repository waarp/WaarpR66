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
package openr66.protocol.networkhandler.ssl;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.security.KeyStore;
import java.security.Security;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * SecureSslContextFactory for SSL
 *
 * @author The Netty Project (netty@googlegroups.com)
 * @author Trustin Lee (trustin@gmail.com)
 *
 * @version $Rev$, $Date$
 *
 */
public class SecureSslContextFactory {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(SecureSslContextFactory.class);
    /**
	 *
	 */
    private static final String PROTOCOL = "TLS";

    /**
	 *
	 */
    private static final SSLContext SERVER_CONTEXT;

    /**
	 *
	 */
    private static final SSLContext CLIENT_CONTEXT;

    static {
        String algorithm = Security
                .getProperty("ssl.KeyManagerFactory.algorithm");
        if (algorithm == null) {
            algorithm = "SunX509";
        }

        SSLContext serverContext = null;
        SSLContext clientContext = null;
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            if (SecureKeyStore.cert == null) {
                ks.load(SecureKeyStore.asInputStream(), SecureKeyStore
                        .getKeyStorePassword());
            } else {
                ks.setCertificateEntry("openr66", SecureKeyStore.cert);
            }

            // Set up key manager factory to use our key store
            KeyManagerFactory kmfs = KeyManagerFactory.getInstance(algorithm);
            kmfs.init(ks, SecureKeyStore.getCertificatePassword());

            // Initialize the SSLContext to work with our key managers.
            serverContext = SSLContext.getInstance(PROTOCOL);
            serverContext.init(kmfs.getKeyManagers(), SecureTrustManagerFactory
                    .getTrustManagers(), null);
        } catch (Exception e) {
            logger.error("Failed to initialize the server-side SSLContext",
                    e);
            throw new Error("Failed to initialize the server-side SSLContext",
                    e);
        }

        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            if (SecureKeyStore.cert == null) {
                ks.load(SecureKeyStore.asInputStream(), SecureKeyStore
                        .getKeyStorePassword());
            } else {
                ks.setCertificateEntry("openr66", SecureKeyStore.cert);
            }
            // Set up key manager factory to use our key store
            KeyManagerFactory kmfc = KeyManagerFactory.getInstance(algorithm);
            kmfc.init(ks, SecureKeyStore.getCertificatePassword());

            clientContext = SSLContext.getInstance(PROTOCOL);
            /*
             * clientContext.init(null, SecureTrustManagerFactory
             * .getTrustManagers(), null);
             */
            clientContext.init(kmfc.getKeyManagers(), SecureTrustManagerFactory
                    .getTrustManagers(), null);
        } catch (Exception e) {
            logger.error("Failed to initialize the client-side SSLContext",
                    e);
            throw new Error("Failed to initialize the client-side SSLContext",
                    e);
        }

        SERVER_CONTEXT = serverContext;
        CLIENT_CONTEXT = clientContext;
    }

    /**
     * @return the Server Context
     */
    public static SSLContext getServerContext() {
        return SERVER_CONTEXT;
    }

    /**
     * @return the Client Context
     */
    public static SSLContext getClientContext() {
        return CLIENT_CONTEXT;
    }
}
