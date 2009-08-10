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
            logger.warn("gai");
            if (SecureKeyStore.cert != null) {
                X509Certificate[] array = new X509Certificate[1];
                array[0] = SecureKeyStore.cert;
                return array;
            }
            return new X509Certificate[0];
        }

        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {
            // Always trust - it's an example.
            // You should do something in the real world.
            logger.warn("cct:" + arg0.length + ":" + arg1);
            if (arg0[0] != null) {
                logger.warn("RSA1:" + arg0[0].toString());
            }

            arg0[0].checkValidity();
            Principal issuer = arg0[0].getIssuerDN();
            BigInteger serial = arg0[0].getSerialNumber();
            logger.warn("issuer:" +
                    issuer.toString().equals(SecureKeyStore.issuer));
            logger.warn("issuer:" + issuer);
            logger.warn("serial:" + serial.toString());
            logger.warn("serial:" + serial.equals(SecureKeyStore.serial));
        }

        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {
            // Always trust - it's an example.
            // You should do something in the real world.
            logger.warn("cst:" + arg0.length + ":" + arg1);
            if (arg0[0] != null) {
                logger.warn("RSA1:" + arg0[0].toString());
            }
            arg0[0].checkValidity();
            Principal issuer = arg0[0].getIssuerDN();
            BigInteger serial = arg0[0].getSerialNumber();
            logger.warn("issuer:" +
                    issuer.toString().equals(SecureKeyStore.issuer));
            logger.warn("issuer:" + issuer);
            logger.warn("serial:" + serial.equals(SecureKeyStore.serial));
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
