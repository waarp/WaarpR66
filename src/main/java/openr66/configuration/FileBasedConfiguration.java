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
package openr66.configuration;

import goldengate.common.crypto.Des;
import goldengate.common.crypto.ssl.GgSecureKeyStore;
import goldengate.common.crypto.ssl.GgSslContextFactory;
import goldengate.common.digest.FilesystemBasedDigest;
import goldengate.common.digest.MD5;
import goldengate.common.exception.CryptoException;
import goldengate.common.file.DirInterface;
import goldengate.common.file.filesystembased.FilesystemBasedDirImpl;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdk5;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdk6;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdkAbstract;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import openr66.context.authentication.R66Auth;
import openr66.context.filesystem.R66Dir;
import openr66.context.task.localexec.LocalExecClient;
import openr66.database.DbAdmin;
import openr66.database.DbConstant;
import openr66.database.data.DbConfiguration;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.http.adminssl.HttpSslPipelineFactory;
import openr66.protocol.networkhandler.ssl.NetworkSslServerPipelineFactory;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.jboss.netty.handler.traffic.AbstractTrafficShapingHandler;

/**
 * File Based Configuration
 *
 * @author frederic bregier
 *
 */
public class FileBasedConfiguration {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(FileBasedConfiguration.class);

    /**
     * SERVER HOSTID
     */
    private static final String XML_SERVER_HOSTID = "/config/identity/hostid";

    /**
     * SERVER SSL HOSTID
     */
    private static final String XML_SERVER_SSLHOSTID = "/config/identity/sslhostid";

    /**
     * ADMINISTRATOR SERVER NAME (shutdown)
     */
    private static final String XML_SERVER_ADMIN = "/config/identity/serveradmin";

    /**
     * SERVER PASSWORD (shutdown)
     */
    private static final String XML_SERVER_PASSWD = "/config/identity/serverpasswd";

    /**
     * SERVER PORT
     */
    private static final String XML_SERVER_PORT = "/config/network/serverport";

    /**
     * SERVER SSL PORT
     */
    private static final String XML_SERVER_SSLPORT = "/config/network/serversslport";

    /**
     * SERVER HTTP PORT
     */
    private static final String XML_SERVER_HTTPPORT = "/config/network/serverhttpport";

    /**
     * SERVER HTTP PORT
     */
    private static final String XML_SERVER_HTTPSPORT = "/config/network/serverhttpsport";

    /**
     * SERVER SSL STOREKEY PATH
     */
    private static final String XML_PATH_KEYPATH = "/config/ssl/keypath";

    /**
     * SERVER SSL KEY PASS
     */
    private static final String XML_PATH_KEYPASS = "/config/ssl/keypass";

    /**
     * SERVER SSL STOREKEY PASS
     */
    private static final String XML_PATH_KEYSTOREPASS = "/config/ssl/keystorepass";

    /**
     * SERVER SSL TRUSTSTOREKEY PATH
     */
    private static final String XML_PATH_TRUSTKEYPATH = "/config/ssl/trustkeypath";

    /**
     * SERVER SSL TRUSTSTOREKEY PASS
     */
    private static final String XML_PATH_TRUSTKEYSTOREPASS = "/config/ssl/trustkeystorepass";

    /**
     * SERVER SSL STOREKEY PATH ADMIN
     */
    private static final String XML_PATH_ADMIN_KEYPATH = "/config/ssl/admkeypath";

    /**
     * SERVER SSL KEY PASS ADMIN
     */
    private static final String XML_PATH_ADMIN_KEYPASS = "/config/ssl/admkeypass";

    /**
     * SERVER SSL STOREKEY PASS ADMIN
     */
    private static final String XML_PATH_ADMIN_KEYSTOREPASS = "/config/ssl/admkeystorepass";

    /**
     * SERVER CRYPTO for Password
     */
    private static final String XML_PATH_CRYPTOKEY = "/config/ssl/cryptokey";

    /**
     * Base Directory
     */
    private static final String XML_SERVER_HOME = "/config/directory/serverhome";

    /**
     * IN Directory
     */
    private static final String XML_INPATH = "/config/directory/in";

    /**
     * OUT Directory
     */
    private static final String XML_OUTPATH = "/config/directory/out";

    /**
     * ARCHIVE Directory
     */
    private static final String XML_ARCHIVEPATH = "/config/directory/arch";

    /**
     * WORKING Directory
     */
    private static final String XML_WORKINGPATH = "/config/directory/work";

    /**
     * CONFIG Directory
     */
    private static final String XML_CONFIGPATH = "/config/directory/conf";

    /**
     * HTTP Admin Directory
     */
    private static final String XML_HTTPADMINPATH = "/config/directory/httpadmin";

    /**
     * Use SSL for R66 connection
     */
    private static final String XML_USESSL = "/config/parameter/usessl";

    /**
     * Use non SSL for R66 connection
     */
    private static final String XML_USENOSSL = "/config/parameter/usenossl";

    /**
     * Use HTTP compression for R66 HTTP connection
     */
    private static final String XML_USEHTTPCOMP = "/config/parameter/usehttpcomp";

    /**
     * SERVER SSL Use TrustStore for Client Authentication
     */
    private static final String XML_USECLIENT_AUTHENT = "/config/parameter/trustuseclientauthenticate";

    /**
     * Use external GoldenGate Local Exec for ExecTask and ExecMoveTask
     */
    private static final String XML_USELOCALEXEC = "/config/parameter/uselocalexec";

    /**
     * Address of GoldenGate Local Exec for ExecTask and ExecMoveTask
     */
    private static final String XML_LEXECADDR = "/config/parameter/lexecaddr";

    /**
     * Port of GoldenGate Local Exec for ExecTask and ExecMoveTask
     */
    private static final String XML_LEXECPORT = "/config/parameter/lexecport";

    /**
     * Default number of threads in pool for Server.
     */
    private static final String XML_SERVER_THREAD = "/config/parameter/serverthread";

    /**
     * Default number of threads in pool for Client (truly concurrent).
     */
    private static final String XML_CLIENT_THREAD = "/config/parameter/clientthread";

    /**
     * Memory Limit to use.
     */
    private static final String XML_MEMORY_LIMIT = "/config/parameter/memorylimit";

    /**
     * Limit per session
     */
    private static final String XML_LIMITSESSION = "/config/parameter/sessionlimit";

    /**
     * Limit global
     */
    private static final String XML_LIMITGLOBAL = "/config/parameter/globallimit";

    /**
     * Delay between two checks for Limit
     */
    private static final String XML_LIMITDELAY = "/config/parameter/delaylimit";

    /**
     * Limit of number of active Runner from Commander
     */
    private static final String XML_LIMITRUNNING = "/config/parameter/runlimit";

    /**
     * Delay between two checks for Commander
     */
    private static final String XML_DELAYCOMMANDER = "/config/parameter/delaycommand";

    /**
     * Delay between two checks for Commander
     */
    private static final String XML_DELAYRETRY = "/config/parameter/delayretry";

    /**
     * Nb of milliseconds after connection is in timeout
     */
    private static final String XML_TIMEOUTCON = "/config/parameter/timeoutcon";

    /**
     * Should a file MD5 SHA1 be computed using NIO
     */
    private static final String XML_USENIO = "/config/parameter/usenio";

    /**
     * Should a file MD5 be computed using FastMD5
     */
    private static final String XML_USEFASTMD5 = "/config/parameter/usefastmd5";

    /**
     * If using Fast MD5, should we used the binary JNI library, empty meaning
     * no
     */
    private static final String XML_FASTMD5 = "/config/parameter/fastmd5";

    /**
     * Size by default of block size for receive/sending files. Should be a
     * multiple of 8192 (maximum = 64K due to block limitation to 2 bytes)
     */
    private static final String XML_BLOCKSIZE = "/config/parameter/blocksize";

    /**
     * Database Driver as of oracle, mysql, postgresql, h2
     */
    private static final String XML_DBDRIVER = "/config/db/dbdriver";

    /**
     * Database Server connection string as of
     * jdbc:type://[host:port],[failoverhost:port]
     * .../[database][?propertyName1][
     * =propertyValue1][&propertyName2][=propertyValue2]...
     */
    private static final String XML_DBSERVER = "/config/db/dbserver";

    /**
     * Database User
     */
    private static final String XML_DBUSER = "/config/db/dbuser";

    /**
     * Database Password
     */
    private static final String XML_DBPASSWD = "/config/db/dbpasswd";

    /**
     * Authentication
     */
    private static final String XML_AUTHENTIFICATION_FILE = "/config/authentfile";

    /**
     *
     * @param document
     * @param fromXML
     * @return the new subpath
     * @throws OpenR66ProtocolSystemException
     */
    private static String getSubPath(Document document, String fromXML)
            throws OpenR66ProtocolSystemException {
        Node node = document.selectSingleNode(fromXML);
        if (node == null) {
            logger.error("Unable to find a Path in Config file: "+fromXML);
            throw new OpenR66ProtocolSystemException(
                    "Unable to find a Path in Config file: " + fromXML);
        }

        String path = node.getText();
        if (path == null || path.length() == 0) {
            throw new OpenR66ProtocolSystemException(
                    "Unable to find a correct Path in Config file: " + fromXML);
        }
        path = DirInterface.SEPARATOR + path;
        String newpath = Configuration.configuration.baseDirectory + path;
        File file = new File(newpath);
        if (!file.isDirectory()) {
            FileUtils.createDir(file);
        }
        return path;
    }

    /**
     * Read a boolean value (0,1,true,false) from a node
     * @param node
     * @return the corresponding value
     */
    private static boolean getBoolean(Node node) {
        String val = node.getText();
        boolean bval;
        try {
            int ival = Integer.parseInt(val);
            bval = (ival == 1) ? true : false;
        } catch (NumberFormatException e) {
            bval = Boolean.parseBoolean(val);
        }
        return bval;
    }
    /**
     * Initiate the configuration from the xml file for server
     *
     * @param filename
     * @return True if OK
     */
    public static boolean setConfigurationFromXml(String filename) {
        Document document = null;
        // Open config file
        try {
            document = new SAXReader().read(filename);
        } catch (DocumentException e) {
            logger.error("Unable to read the XML Config file: " + filename, e);
            return false;
        }
        if (document == null) {
            logger.error("Unable to read the XML Config file: " + filename);
            return false;
        }
        Node node = document.selectSingleNode(XML_USESSL);
        if (node != null) {
            Configuration.configuration.useSSL = getBoolean(node);
        }
        node = document.selectSingleNode(XML_USENOSSL);
        if (node != null) {
            Configuration.configuration.useNOSSL = getBoolean(node);
        }
        node = document.selectSingleNode(XML_USEHTTPCOMP);
        if (node != null) {
            Configuration.configuration.useHttpCompression = getBoolean(node);
        }
        node = document.selectSingleNode(XML_USELOCALEXEC);
        if (node != null) {
            Configuration.configuration.useLocalExec = getBoolean(node);
            if (Configuration.configuration.useLocalExec) {
                node = document.selectSingleNode(XML_LEXECADDR);
                String saddr;
                InetAddress addr;
                if (node == null) {
                    logger.warn("Unable to find LocalExec Address in Config file: " + filename);
                    try {
                        addr = InetAddress.getByAddress(new byte[]{127,0,0,1});
                    } catch (UnknownHostException e) {
                        logger.error("Unable to find LocalExec Address in Config file: " + filename);
                        return false;
                    }
                } else {
                    saddr = node.getText();
                    try {
                        addr = InetAddress.getByName(saddr);
                    } catch (UnknownHostException e) {
                        logger.error("Unable to find LocalExec Address in Config file: " + filename);
                        return false;
                    }
                }
                node = document.selectSingleNode(XML_LEXECPORT);
                int port = 9999;
                if (node != null) {
                    port = Integer.parseInt(node.getText());
                }
                LocalExecClient.address = new InetSocketAddress(addr, port);
            }
        }
        if (!loadCommon(document)) {
            logger.error("Unable to find Host ID in Config file: " + filename);
            return false;
        }
        node = document.selectSingleNode(XML_HTTPADMINPATH);
        if (node == null) {
            logger.error("Unable to find Http Admin Base in Config file");
            return false;
        }

        String path = node.getText();
        if (path == null || path.length() == 0) {
            logger.error("Unable to set correct Http Admin Base in Config file");
            return false;
        }
        path = DirInterface.SEPARATOR + path;
        Configuration.configuration.httpBasePath =
            FilesystemBasedDirImpl.normalizePath(path);
        node = document.selectSingleNode(XML_SERVER_PORT);
        int port = 6666;
        if (node != null) {
            port = Integer.parseInt(node.getText());
        }
        Configuration.configuration.SERVER_PORT = port;
        node = document.selectSingleNode(XML_SERVER_SSLPORT);
        int sslport = 6667;
        if (node != null) {
            sslport = Integer.parseInt(node.getText());
        }
        Configuration.configuration.SERVER_SSLPORT = sslport;
        node = document.selectSingleNode(XML_SERVER_HTTPPORT);
        int httpport = 8066;
        if (node != null) {
            httpport = Integer.parseInt(node.getText());
        }
        Configuration.configuration.SERVER_HTTPPORT = httpport;
        node = document.selectSingleNode(XML_SERVER_HTTPSPORT);
        int httpsport = 8067;
        if (node != null) {
            httpsport = Integer.parseInt(node.getText());
        }
        Configuration.configuration.SERVER_HTTPSPORT = httpsport;

        node = document.selectSingleNode(XML_SERVER_ADMIN);
        if (node == null) {
            logger.error("Unable to find Administrator name in Config file: " + filename);
            return false;
        }
        Configuration.configuration.ADMINNAME = node.getText();
        node = document.selectSingleNode(XML_SERVER_PASSWD);
        if (node == null) {
            logger.error("Unable to find Password in Config file: " + filename);
            return false;
        }
        String passwd = node.getText();
        byte[] decodedByteKeys = null;
        try {
            decodedByteKeys =
                Configuration.configuration.cryptoKey.decryptHexInBytes(passwd);
        } catch (Exception e) {
            logger.error(
                    "Unable to Decrypt Server Password in Config file from: " +
                            passwd, e);
            return false;
        }
        Configuration.configuration.setSERVERKEY(decodedByteKeys);

        if (!loadDatabase(document)) {
            return false;
        }
        if (!loadFromDatabase(document)) {
            return false;
        }
        if (!DbConstant.admin.isConnected) {
            // if no database, must load authentication from file
            node = document.selectSingleNode(XML_AUTHENTIFICATION_FILE);
            if (node == null) {
                logger
                        .warn("Unable to find Authentication file in Config file: " +
                                filename);
                return false;
            } else {
                String fileauthent = node.getText();
                document = null;
                if (!AuthenticationFileBasedConfiguration
                        .loadAuthentication(fileauthent)) {
                    return false;
                }
            }
        }
        Configuration.configuration.HOST_AUTH = R66Auth.getServerAuth(
                DbConstant.admin.session, Configuration.configuration.HOST_ID);
        if (Configuration.configuration.HOST_AUTH == null &&
                Configuration.configuration.useNOSSL) {
            logger.error("Cannot find Authentication for current host");
            return false;
        }
        if (Configuration.configuration.HOST_SSLID != null) {
            Configuration.configuration.HOST_SSLAUTH = R66Auth.getServerAuth(
                    DbConstant.admin.session,
                    Configuration.configuration.HOST_SSLID);
            if (Configuration.configuration.HOST_SSLAUTH == null &&
                    Configuration.configuration.useSSL) {
                logger.error("Cannot find SSL Authentication for current host");
                return false;
            }
        }
        return true;
    }

    /**
     * Initiate the configuration from the xml file for database client
     *
     * @param filename
     * @return True if OK
     */
    public static boolean setClientConfigurationFromXml(String filename) {
        Document document = null;
        // Open config file
        try {
            document = new SAXReader().read(filename);
        } catch (DocumentException e) {
            logger.error("Unable to read the XML Config file: " + filename, e);
            return false;
        }
        if (document == null) {
            logger.error("Unable to read the XML Config file: " + filename);
            return false;
        }
        // Client enables SSL by default but could be reverted later on
        Configuration.configuration.useSSL = true;
        if (!loadCommon(document)) {
            logger.error("Unable to load commons in Config file: " + filename);
            return false;
        }
        Node node = null;
        if (!loadDatabase(document)) {
            return false;
        }
        if (!loadFromDatabase(document)) {
            return false;
        }

        if (!DbConstant.admin.isConnected) {
            // if no database, must load authentication from file
            node = document.selectSingleNode(XML_AUTHENTIFICATION_FILE);
            if (node == null) {
                logger
                        .warn("Unable to find Authentication file in Config file: " +
                                filename);
                return false;
            } else {
                String fileauthent = node.getText();
                document = null;
                if (!AuthenticationFileBasedConfiguration
                        .loadAuthentication(fileauthent)) {
                    return false;
                }
            }
        }
        Configuration.configuration.HOST_AUTH = R66Auth.getServerAuth(
                DbConstant.admin.session, Configuration.configuration.HOST_ID);
        if (Configuration.configuration.HOST_AUTH == null) {
            logger.error("Cannot find Authentication for current host");
            return false;
        }
        if (Configuration.configuration.HOST_SSLID != null) {
            Configuration.configuration.HOST_SSLAUTH = R66Auth.getServerAuth(
                    DbConstant.admin.session,
                    Configuration.configuration.HOST_SSLID);
            if (Configuration.configuration.HOST_SSLAUTH == null) {
                logger.error("Cannot find SSL Authentication for current host");
                return false;
            }
        }
        return true;
    }

    /**
     * Load common configuration from XML document
     *
     * @param document
     * @return True if OK
     */
    public static boolean loadCommon(Document document) {
        Node node = null;
        node = document.selectSingleNode(XML_SERVER_HOSTID);
        if (node == null) {
            logger.error("Unable to find Host ID in Config file");
            return false;
        }
        Configuration.configuration.HOST_ID = node.getText();
        node = document.selectSingleNode(XML_SERVER_SSLHOSTID);
        if (node == null) {
            logger
                    .warn("Unable to find Host SSL ID in Config file so no SSL support will be used");
            Configuration.configuration.useSSL = false;
            Configuration.configuration.HOST_SSLID = null;
        } else {
            Configuration.configuration.HOST_SSLID = node.getText();
        }
        node = document.selectSingleNode(XML_SERVER_HOME);
        if (node == null) {
            logger.error("Unable to find Home in Config file");
            return false;
        }
        String path = node.getText();
        File file = new File(path);
        if (!file.isDirectory()) {
            logger.error("Home is not a directory in Config file");
            return false;
        }
        try {
            Configuration.configuration.baseDirectory = FilesystemBasedDirImpl
                    .normalizePath(file.getCanonicalPath());
        } catch (IOException e1) {
            logger.error("Unable to set Home in Config file");
            return false;
        }
        try {
            Configuration.configuration.configPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_CONFIGPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Config in Config file");
            return false;
        }
        try {
            Configuration.configuration.inPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_INPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set In in Config file");
            return false;
        }
        try {
            Configuration.configuration.outPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_OUTPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Out in Config file");
            return false;
        }
        try {
            Configuration.configuration.workingPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_WORKINGPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Working in Config file");
            return false;
        }
        try {
            Configuration.configuration.archivePath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_ARCHIVEPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Archive in Config file");
            return false;
        }
        node = document.selectSingleNode(XML_SERVER_THREAD);
        if (node != null) {
            Configuration.configuration.SERVER_THREAD = Integer.parseInt(node
                    .getText());
        }
        node = document.selectSingleNode(XML_CLIENT_THREAD);
        if (node != null) {
            Configuration.configuration.CLIENT_THREAD = Integer.parseInt(node
                    .getText());
        }
        node = document.selectSingleNode(XML_MEMORY_LIMIT);
        if (node != null) {
            Configuration.configuration.maxGlobalMemory = Integer.parseInt(node.getText());
        }
        Configuration.getFileParameter().deleteOnAbort = false;
        node = document.selectSingleNode(XML_USENIO);
        if (node != null) {
            FilesystemBasedFileParameterImpl.useNio = getBoolean(node);
        }
        node = document.selectSingleNode(XML_USEFASTMD5);
        if (node != null) {
            FilesystemBasedDigest.useFastMd5 = getBoolean(node);
            if (FilesystemBasedDigest.useFastMd5) {
                node = document.selectSingleNode(XML_FASTMD5);
                if (node != null) {
                    FilesystemBasedDigest.fastMd5Path = node.getText();
                    if (FilesystemBasedDigest.fastMd5Path == null ||
                            FilesystemBasedDigest.fastMd5Path.length() == 0) {
                        logger.info("FastMD5 init lib to null");
                        FilesystemBasedDigest.fastMd5Path = null;
                    } else {
                        logger.info("FastMD5 init lib to {}",
                                FilesystemBasedDigest.fastMd5Path);
                        MD5
                                .initNativeLibrary(FilesystemBasedDigest.fastMd5Path);
                    }
                }
            } else {
                FilesystemBasedDigest.fastMd5Path = null;
            }
        }
        node = document.selectSingleNode(XML_BLOCKSIZE);
        if (node != null) {
            Configuration.configuration.BLOCKSIZE = Integer.parseInt(node
                    .getText());
        }
        node = document.selectSingleNode(XML_TIMEOUTCON);
        if (node != null) {
            Configuration.configuration.TIMEOUTCON = Integer.parseInt(node
                    .getText());
        }
        if (Configuration.USEJDK6) {
            R66Dir.initJdkDependent(new FilesystemBasedDirJdk6());
        } else {
            R66Dir.initJdkDependent(new FilesystemBasedDirJdk5());
        }

        // Key for OpenR66 server
        if (Configuration.configuration.useSSL) {
            node = document.selectSingleNode(XML_PATH_KEYPATH);
            if (node == null) {
                logger.info("Unable to find Key Path");
                try {
                    NetworkSslServerPipelineFactory.ggSecureKeyStore =
                        new GgSecureKeyStore("secret", "secret");
                } catch (CryptoException e) {
                    logger.error("Bad SecureKeyStore construction");
                    return false;
                }
            } else {
                String keypath = node.getText();
                if ((keypath == null) || (keypath.length() == 0)) {
                    logger.error("Bad Key Path");
                    return false;
                }
                node = document.selectSingleNode(XML_PATH_KEYSTOREPASS);
                if (node == null) {
                    logger.error("Unable to find KeyStore Passwd");
                    return false;
                }
                String keystorepass = node.getText();
                if ((keystorepass == null) || (keystorepass.length() == 0)) {
                    logger.error("Bad KeyStore Passwd");
                    return false;
                }
                node = document.selectSingleNode(XML_PATH_KEYPASS);
                if (node == null) {
                    logger.error("Unable to find Key Passwd");
                    return false;
                }
                String keypass = node.getText();
                if ((keypass == null) || (keypass.length() == 0)) {
                    logger.error("Bad Key Passwd");
                    return false;
                }
                try {
                    NetworkSslServerPipelineFactory.ggSecureKeyStore =
                        new GgSecureKeyStore(keypath, keystorepass,
                                keypass);
                } catch (CryptoException e) {
                    logger.error("Bad SecureKeyStore construction");
                    return false;
                }

            }
            // TrustedKey for OpenR66 server
            node = document.selectSingleNode(XML_PATH_TRUSTKEYPATH);
            if (node == null) {
                logger.info("Unable to find TRUST Key Path");
                try {
                    NetworkSslServerPipelineFactory.ggSecureKeyStore.initEmptyTrustStore();
                } catch (CryptoException e) {
                    logger.error("Bad TrustKeyStore construction");
                    return false;
                }
            } else {
                String keypath = node.getText();
                if ((keypath == null) || (keypath.length() == 0)) {
                    logger.error("Bad TRUST Key Path");
                    return false;
                }
                node = document.selectSingleNode(XML_PATH_TRUSTKEYSTOREPASS);
                if (node == null) {
                    logger.error("Unable to find TRUST KeyStore Passwd");
                    return false;
                }
                String keystorepass = node.getText();
                if ((keystorepass == null) || (keystorepass.length() == 0)) {
                    logger.error("Bad TRUST KeyStore Passwd");
                    return false;
                }
                node = document.selectSingleNode(XML_USECLIENT_AUTHENT);
                boolean useClientAuthent = false;
                if (node != null) {
                    useClientAuthent = getBoolean(node);
                }
                try {
                    NetworkSslServerPipelineFactory.ggSecureKeyStore.initTrustStore(keypath,
                            keystorepass, useClientAuthent);
                } catch (CryptoException e) {
                    logger.error("Bad TrustKeyStore construction");
                    return false;
                }
            }
            NetworkSslServerPipelineFactory.ggSslContextFactory =
                new GgSslContextFactory(
                        NetworkSslServerPipelineFactory.ggSecureKeyStore);
        }

        // Key for HTTPS
        node = document.selectSingleNode(XML_PATH_ADMIN_KEYPATH);
        if (node != null) {
            String keypath = node.getText();
            if ((keypath == null) || (keypath.length() == 0)) {
                logger.error("Bad Key Path");
                return false;
            }
            node = document.selectSingleNode(XML_PATH_ADMIN_KEYSTOREPASS);
            if (node == null) {
                logger.error("Unable to find KeyStore Passwd");
                return false;
            }
            String keystorepass = node.getText();
            if ((keystorepass == null) || (keystorepass.length() == 0)) {
                logger.error("Bad KeyStore Passwd");
                return false;
            }
            node = document.selectSingleNode(XML_PATH_ADMIN_KEYPASS);
            if (node == null) {
                logger.error("Unable to find Key Passwd");
                return false;
            }
            String keypass = node.getText();
            if ((keypass == null) || (keypass.length() == 0)) {
                logger.error("Bad Key Passwd");
                return false;
            }
            try {
                HttpSslPipelineFactory.ggSecureKeyStore =
                    new GgSecureKeyStore(keypath, keystorepass,
                            keypass);
            } catch (CryptoException e) {
                logger.error("Bad SecureKeyStore construction for AdminSsl");
                return false;
            }
            // No client authentication
            try {
                HttpSslPipelineFactory.ggSecureKeyStore.initEmptyTrustStore();
            } catch (CryptoException e) {
                logger.error("Bad TrustKeyStore construction");
                return false;
            }
            HttpSslPipelineFactory.ggSslContextFactory =
                new GgSslContextFactory(
                        HttpSslPipelineFactory.ggSecureKeyStore, true);
        }

        if (!setCryptoKey(document)) {
            return false;
        }

        // We use Apache Commons IO
        FilesystemBasedDirJdkAbstract.ueApacheCommonsIo = true;
        return true;
    }

    /**
     * Set the Crypto Key from the Document
     * @param document
     * @return True if OK
     */
    public static boolean setCryptoKey(Document document) {
        Node node = document.selectSingleNode(XML_PATH_CRYPTOKEY);
        if (node == null) {
            logger.error("Unable to find CryptoKey in Config file");
            return false;
        }
        String filename = node.getText();
        File key = new File(filename);
        Des des = new Des();
        try {
            des.setSecretKey(key);
        } catch (CryptoException e) {
            logger.error("Unable to load CryptoKey from Config file");
            return false;
        } catch (IOException e) {
            logger.error("Unable to load CryptoKey from Config file");
            return false;
        }
        Configuration.configuration.cryptoKey = des;
        return true;
    }
    /**
     * Load data from database or from files if not connected
     *
     * @param document
     * @return True if OK
     */
    private static boolean loadFromDatabase(Document document) {
        if (DbConstant.admin.isConnected) {
            // load from database the limit to apply
            try {
                DbConfiguration configuration = new DbConfiguration(
                        DbConstant.admin.session,
                        Configuration.configuration.HOST_ID);
                configuration.updateConfiguration();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot load configuration from database", e);
            }
        } else {
            if (Configuration.configuration.baseDirectory != null &&
                    Configuration.configuration.configPath != null) {
                // load Rules from files
                File dirConfig = new File(
                        Configuration.configuration.baseDirectory +
                                Configuration.configuration.configPath);
                if (dirConfig.isDirectory()) {
                    try {
                        RuleFileBasedConfiguration.importRules(dirConfig);
                    } catch (OpenR66ProtocolSystemException e) {
                        logger.error("Cannot load Rules", e);
                        return false;
                    } catch (OpenR66DatabaseException e) {
                        logger.error("Cannot load Rules", e);
                        return false;
                    }
                } else {
                    logger.error("Config Directory is not a directory: " +
                            Configuration.configuration.baseDirectory +
                            Configuration.configuration.configPath);
                    return false;
                }
            }
            // load if possible the limit to apply
            loadLimit(document);
        }
        return true;
    }

    /**
     * Load database parameter
     *
     * @param document
     * @return True if OK
     */
    public static boolean loadDatabase(Document document) {
        Node node = document.selectSingleNode(XML_DBDRIVER);
        if (node == null) {
            logger.error("Unable to find DBDriver in Config file");
            DbConstant.admin = new DbAdmin(); // no database support
        } else {
            String dbdriver = node.getText();
            node = document.selectSingleNode(XML_DBSERVER);
            if (node == null) {
                logger.error("Unable to find DBServer in Config file");
                return false;
            }
            String dbserver = node.getText();
            node = document.selectSingleNode(XML_DBUSER);
            if (node == null) {
                logger.error("Unable to find DBUser in Config file");
                return false;
            }
            String dbuser = node.getText();
            node = document.selectSingleNode(XML_DBPASSWD);
            if (node == null) {
                logger.error("Unable to find DBPassword in Config file");
                return false;
            }
            String dbpasswd = node.getText();
            if (dbdriver == null || dbserver == null || dbuser == null ||
                    dbpasswd == null || dbdriver.length() == 0 ||
                    dbserver.length() == 0 || dbuser.length() == 0 ||
                    dbpasswd.length() == 0) {
                logger.error("Unable to find Correct DB data in Config file");
                return false;
            }
            try {
                DbModelFactory.initialize(dbdriver, dbserver, dbuser, dbpasswd,
                        true);
            } catch (OpenR66DatabaseNoConnectionError e2) {
                logger.error("Unable to Connect to DB", e2);
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param document
     * @return True if the load of the limit is ok
     */
    public static boolean loadLimit(Document document) {
        // should be removed and set from database
        Node node = document.selectSingleNode(XML_LIMITGLOBAL);
        if (node != null) {
            Configuration.configuration.serverGlobalReadLimit = Long
                    .parseLong(node.getText());
            if (Configuration.configuration.serverGlobalReadLimit <= 0) {
                Configuration.configuration.serverGlobalReadLimit = 0;
            }
            Configuration.configuration.serverGlobalWriteLimit = Configuration.configuration.serverGlobalReadLimit;
            logger.info("Global Limit: {}",
                    Configuration.configuration.serverGlobalReadLimit);
        }
        node = document.selectSingleNode(XML_LIMITSESSION);
        if (node != null) {
            Configuration.configuration.serverChannelReadLimit = Long
                    .parseLong(node.getText());
            if (Configuration.configuration.serverChannelReadLimit <= 0) {
                Configuration.configuration.serverChannelReadLimit = 0;
            }
            Configuration.configuration.serverChannelWriteLimit = Configuration.configuration.serverChannelReadLimit;
            logger.info("SessionInterface Limit: {}",
                    Configuration.configuration.serverChannelReadLimit);
        }
        Configuration.configuration.delayLimit = AbstractTrafficShapingHandler.DEFAULT_CHECK_INTERVAL;
        node = document.selectSingleNode(XML_LIMITDELAY);
        if (node != null) {
            Configuration.configuration.delayLimit = Long.parseLong(node
                    .getText());
            if (Configuration.configuration.delayLimit <= 0) {
                Configuration.configuration.delayLimit = 0;
            }
            logger.info("Delay Limit: {}",
                    Configuration.configuration.delayLimit);
        }
        node = document.selectSingleNode(XML_LIMITRUNNING);
        if (node != null) {
            Configuration.configuration.RUNNER_THREAD = Integer.parseInt(node
                    .getText());
            if (Configuration.configuration.RUNNER_THREAD < 10) {
                Configuration.configuration.RUNNER_THREAD = 10;
            }
            logger.info("Limit of Runner: {}",
                    Configuration.configuration.RUNNER_THREAD);
        }
        node = document.selectSingleNode(XML_DELAYCOMMANDER);
        if (node != null) {
            Configuration.configuration.delayCommander = Long.parseLong(node
                    .getText());
            if (Configuration.configuration.delayCommander <= 100) {
                Configuration.configuration.delayCommander = 100;
            }
            logger.info("Delay Commander: {}",
                    Configuration.configuration.delayCommander);
        }
        node = document.selectSingleNode(XML_DELAYRETRY);
        if (node != null) {
            Configuration.configuration.delayRetry = Long.parseLong(node
                    .getText());
            if (Configuration.configuration.delayRetry <= 1000) {
                Configuration.configuration.delayRetry = 1000;
            }
            logger.info("Delay Retry: {}",
                    Configuration.configuration.delayRetry);
        }
        if (DbConstant.admin.isConnected) {
            node = document.selectSingleNode(XML_SERVER_HOSTID);
            Configuration.configuration.HOST_ID = node.getText();
            DbConfiguration configuration = new DbConfiguration(
                    DbConstant.admin.session,
                    Configuration.configuration.HOST_ID,
                    Configuration.configuration.serverGlobalReadLimit,
                    Configuration.configuration.serverGlobalWriteLimit,
                    Configuration.configuration.serverChannelReadLimit,
                    Configuration.configuration.serverChannelWriteLimit,
                    Configuration.configuration.delayLimit);
            configuration.changeUpdatedInfo(UpdatedInfo.TOSUBMIT);
            try {
                if (configuration.exist()) {
                    configuration.update();
                } else {
                    configuration.insert();
                }
            } catch (OpenR66DatabaseException e) {
            }
        }
        return true;
    }
}
