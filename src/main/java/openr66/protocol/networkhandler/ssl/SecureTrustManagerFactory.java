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

import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Principal;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509TrustManager;

/**
 * SecureTrustManagerFactory for SSL
 *
 * @author The Netty Project (netty@googlegroups.com)
 * @author Trustin Lee (trustin@gmail.com)
 *
 * @version $Rev$, $Date$
 *
 */
public class SecureTrustManagerFactory extends TrustManagerFactorySpi {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(SecureTrustManagerFactory.class);

    /**
	 *
	 */
    private static final TrustManager DUMMY_TRUST_MANAGER = new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
            logger.info("gai");
            return R66SecureKeyStore.x509Array;
        }

        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {
            logger.info("cct:" + arg0.length + ":" + arg1);
            if (arg0[0] == null) {
                logger.error("No certificate");
                throw new CertificateException("No Certificate passed");
            }
            for (int i = 0; i < arg0.length; i++) {
                try {
                    arg0[i].checkValidity();
                } catch (CertificateException e) {
                    // ignore
                    continue;
                }
                BigInteger id = arg0[i].getSerialNumber();
                Principal issuer = arg0[i].getIssuerDN();
                for (int j = 0; j < R66SecureKeyStore.x509Array.length; j++) {
                    BigInteger id2 = R66SecureKeyStore.x509Array[j].getSerialNumber();
                    if ((id2.compareTo(id) == 0) && (issuer.hashCode() ==
                        R66SecureKeyStore.x509Array[j].getIssuerDN().hashCode())) {
                        logger.info("Found Key");
                        return;
                    }
                }
            }
            logger.error("No certificate");
            throw new CertificateException("No certificate found");
        }

        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {
            logger.info("cct:" + arg0.length + ":" + arg1);
            if (arg0[0] == null) {
                logger.error("No certificate");
                throw new CertificateException("No Certificate passed");
            }
            for (int i = 0; i < arg0.length; i++) {
                try {
                    arg0[i].checkValidity();
                } catch (CertificateException e) {
                    // ignore
                    continue;
                }
                BigInteger id = arg0[i].getSerialNumber();
                Principal issuer = arg0[i].getIssuerDN();
                for (int j = 0; j < R66SecureKeyStore.x509Array.length; j++) {
                    BigInteger id2 = R66SecureKeyStore.x509Array[j].getSerialNumber();
                    if ((id2.compareTo(id) == 0) && (issuer.hashCode() ==
                        R66SecureKeyStore.x509Array[j].getIssuerDN().hashCode())) {
                        logger.info("Found Key");
                        return;
                    }
                }
            }
            logger.error("No certificate");
            throw new CertificateException("No certificate found");
        }
    };

    /**
     * @return an array of TrustManagers
     */
    public static TrustManager[] getTrustManagers() {
        return new TrustManager[] {
            DUMMY_TRUST_MANAGER };
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return getTrustManagers();
    }

    @Override
    protected void engineInit(KeyStore keystore) throws KeyStoreException {
        // Unused
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters)
            throws InvalidAlgorithmParameterException {
        // Unused
    }
}
