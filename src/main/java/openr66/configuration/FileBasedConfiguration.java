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

import goldengate.common.digest.FilesystemBasedDigest;
import goldengate.common.digest.MD5;
import goldengate.common.file.DirInterface;
import goldengate.common.file.filesystembased.FilesystemBasedDirImpl;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdk6;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdkAbstract;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import openr66.context.authentication.R66Auth;
import openr66.context.filesystem.R66Dir;
import openr66.database.DbAdmin;
import openr66.database.DbConstant;
import openr66.database.data.DbConfiguration;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.networkhandler.ssl.R66SecureKeyStore;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.jboss.netty.handler.traffic.AbstractTrafficShapingHandler;

/**
 * File Based Configuration
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
    private static final String XML_SERVER_HOSTID = "/config/hostid";

    /**
     * SERVER SSL HOSTID
     */
    private static final String XML_SERVER_SSLHOSTID = "/config/sslhostid";

    /**
     * SERVER PASSWORD (shutdown)
     */
    private static final String XML_SERVER_PASSWD = "/config/serverpasswd";

    /**
     * SERVER PORT
     */
    private static final String XML_SERVER_PORT = "/config/serverport";

    /**
     * SERVER SSL PORT
     */
    private static final String XML_SERVER_SSLPORT = "/config/serversslport";
    /**
     * SERVER HTTP PORT
     */
    private static final String XML_SERVER_HTTPPORT = "/config/serverhttpport";
    /**
     * SERVER SSL KEY PATH
     */
    private static final String XML_PATH_KEYPATH = "/config/keypath";
    /**
     * SERVER SSL KEY PASS
     */
    private static final String XML_PATH_KEYPASS = "/config/keypass";
    /**
     * SERVER SSL KEY PASS
     */
    private static final String XML_PATH_KEYSTOREPASS = "/config/keystorepass";

    /**
     * Base Directory
     */
    private static final String XML_SERVER_HOME = "/config/serverhome";

    /**
     * IN Directory
     */
    private static final String XML_INPATH = "/config/in";

    /**
     * OUT Directory
     */
    private static final String XML_OUTPATH = "/config/out";

    /**
     * ARCHIVE Directory
     */
    private static final String XML_ARCHIVEPATH = "/config/arch";

    /**
     * WORKING Directory
     */
    private static final String XML_WORKINGPATH = "/config/work";

    /**
     * CONFIG Directory
     */
    private static final String XML_CONFIGPATH = "/config/conf";

    /**
     * Default number of threads in pool for Server.
     */
    private static final String XML_SERVER_THREAD = "/config/serverthread";

    /**
     * Default number of threads in pool for Client (truly concurrent).
     */
    private static final String XML_CLIENT_THREAD = "/config/clientthread";

    /**
     * Limit per session
     */
    private static final String XML_LIMITSESSION = "/config/sessionlimit";

    /**
     * Limit global
     */
    private static final String XML_LIMITGLOBAL = "/config/globallimit";
    /**
     * Delay between two checks for Limit
     */
    private static final String XML_LIMITDELAY = "/config/delaylimit";
    /**
     * Limit of number of active Runner from Commander
     */
    private static final String XML_LIMITRUNNING = "/config/runlimit";
    /**
     * Delay between two checks for Commander
     */
    private static final String XML_DELAYCOMMANDER = "/config/delaycommand";

    /**
     * Nb of milliseconds after connection is in timeout
     */
    private static final String XML_TIMEOUTCON = "/config/timeoutcon";

    /**
     * Should a file be deleted when a Store like command is aborted
     */
    private static final String XML_DELETEONABORT = "/config/deleteonabort";

    /**
     * Should a file MD5 SHA1 be computed using NIO
     */
    private static final String XML_USENIO = "/config/usenio";

    /**
     * Should a file MD5 be computed using FastMD5
     */
    private static final String XML_USEFASTMD5 = "/config/usefastmd5";

    /**
     * If using Fast MD5, should we used the binary JNI library, empty meaning
     * no
     */
    private static final String XML_FASTMD5 = "/config/fastmd5";

    /**
     * Size by default of block size for receive/sending files. Should be a
     * multiple of 8192 (maximum = 64K due to block limitation to 2 bytes)
     */
    private static final String XML_BLOCKSIZE = "/config/blocksize";

    /**
     * Database Driver as of oracle, mysql, postgresql, h2
     */
    private static final String XML_DBDRIVER = "/config/dbdriver";

    /**
     * Database Server connection string as of
     * jdbc:type://[host:port],[failoverhost:port]
     * .../[database][?propertyName1][
     * =propertyValue1][&propertyName2][=propertyValue2]...
     */
    private static final String XML_DBSERVER = "/config/dbserver";

    /**
     * Database User
     */
    private static final String XML_DBUSER = "/config/dbuser";

    /**
     * Database Password
     */
    private static final String XML_DBPASSWD = "/config/dbpasswd";

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
            logger.error("Unable to find CONFIG Path in Config file");
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
        if (! loadCommon(document)) {
            logger.error("Unable to find Host ID in Config file: " + filename);
            return false;
        }
        Node node = null;
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

        node = document.selectSingleNode(XML_SERVER_PASSWD);
        if (node == null) {
            logger.error("Unable to find Password in Config file: " + filename);
            return false;
        }
        String passwd = node.getText();
        // load from a file and store as a key
        File key = new File(passwd);
        if (!key.canRead()) {
            logger
                    .error("Unable to Load Server Password in Config file from: " +
                            passwd);
            return false;
        }
        byte[] byteKeys = new byte[(int) key.length()];
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(key);
            inputStream.read(byteKeys);
            inputStream.close();
        } catch (IOException e2) {
            logger.error(
                    "Unable to Load Server Password in Config file from: " +
                            passwd, e2);
            return false;
        }
        Configuration.configuration.setSERVERKEY(byteKeys);

        if (!loadDatabase(document)) {
            return false;
        }
        if (!loadFromDatabase(document)) {
            return false;
        }
        if (! DbConstant.admin.isConnected) {
            // if no database, must load authentication from file
            node = document.selectSingleNode(XML_AUTHENTIFICATION_FILE);
            if (node == null) {
                logger.warn("Unable to find Authentication file in Config file: " +
                        filename);
                return false;
            } else {
                String fileauthent = node.getText();
                document = null;
                if (! AuthenticationFileBasedConfiguration.loadAuthentication(fileauthent)) {
                    return false;
                }
            }
        }
        Configuration.configuration.HOST_AUTH =
            R66Auth.getServerAuth(DbConstant.admin.session,
                    Configuration.configuration.HOST_ID);
        if (Configuration.configuration.HOST_AUTH == null) {
            logger.warn("Cannot find Authentication for current host");
            return false;
        }
        if (Configuration.configuration.HOST_SSLID != null) {
            Configuration.configuration.HOST_SSLAUTH =
                R66Auth.getServerAuth(DbConstant.admin.session,
                        Configuration.configuration.HOST_SSLID);
            if (Configuration.configuration.HOST_SSLAUTH == null) {
                logger.warn("Cannot find SSL Authentication for current host");
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
        if (! loadCommon(document)) {
            logger.error("Unable to find Host ID in Config file: " + filename);
            return false;
        }
        Node node = null;
        if (! loadDatabase(document)) {
            return false;
        }
        if (!loadFromDatabase(document)) {
            return false;
        }

        if (! DbConstant.admin.isConnected) {
            // if no database, must load authentication from file
            node = document.selectSingleNode(XML_AUTHENTIFICATION_FILE);
            if (node == null) {
                logger.warn("Unable to find Authentication file in Config file: " +
                        filename);
                return false;
            } else {
                String fileauthent = node.getText();
                document = null;
                if (! AuthenticationFileBasedConfiguration.loadAuthentication(fileauthent)) {
                    return false;
                }
            }
        }
        Configuration.configuration.HOST_AUTH =
            R66Auth.getServerAuth(DbConstant.admin.session,
                    Configuration.configuration.HOST_ID);
        if (Configuration.configuration.HOST_AUTH == null) {
            logger.warn("Cannot find Authentication for current host");
            return false;
        }
        if (Configuration.configuration.HOST_SSLID != null) {
            Configuration.configuration.HOST_SSLAUTH =
                R66Auth.getServerAuth(DbConstant.admin.session,
                        Configuration.configuration.HOST_SSLID);
            if (Configuration.configuration.HOST_SSLAUTH == null) {
                logger.warn("Cannot find SSL Authentication for current host");
                return false;
            }
        }
        return true;
    }
    /**
     * Load common configuration from XML document
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
            logger.warn("Unable to find Host SSL ID in Config file so no SSL support will be used");
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
            logger
                    .error("Unable to set Config in Config file");
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
        node = document.selectSingleNode(XML_DELETEONABORT);
        if (node != null) {
            Configuration.getFileParameter().deleteOnAbort = Integer
                    .parseInt(node.getText()) == 1? true : false;
        }
        node = document.selectSingleNode(XML_USENIO);
        if (node != null) {
            FilesystemBasedFileParameterImpl.useNio = Integer.parseInt(node
                    .getText()) == 1? true : false;
        }
        node = document.selectSingleNode(XML_USEFASTMD5);
        if (node != null) {
            FilesystemBasedDigest.useFastMd5 = Integer.parseInt(node.getText()) == 1? true
                    : false;
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
        R66Dir.initJdkDependent(new FilesystemBasedDirJdk6());

        // Key
        node = document.selectSingleNode(XML_PATH_KEYPATH);
        if (node == null) {
                logger.warn("Unable to find Key Path");
        } else {
                String keypath = node.getText();
                if ((keypath == null) || (keypath.length() == 0)) {
                        logger.warn("Bad Key Path");
                        return false;
                }
                node = document.selectSingleNode(XML_PATH_KEYPASS);
                if (node == null) {
                        logger.warn("Unable to find Key Passwd");
                        return false;
                }
                String keypass = node.getText();
                if ((keypass == null) || (keypass.length() == 0)) {
                        logger.warn("Bad Key Passwd");
                        return false;
                }
                node = document.selectSingleNode(XML_PATH_KEYSTOREPASS);
                if (node == null) {
                        logger.warn("Unable to find KeyStore Passwd");
                        return false;
                }
                String keystorepass = node.getText();
                if ((keystorepass == null) || (keystorepass.length() == 0)) {
                        logger.warn("Bad KeyStore Passwd");
                        return false;
                }
                if (! R66SecureKeyStore.initSecureKeyStore(keypath, keystorepass, keypass)) {
                        logger.warn("Bad Key");
                        return false;
                }
        }

        // We use Apache Commons IO
        FilesystemBasedDirJdkAbstract.ueApacheCommonsIo = true;
        return true;
    }
    /**
     * Load data from database or from files if not connected
     * @param document
     * @return True if OK
     */
    private static boolean loadFromDatabase(Document document) {
        if (DbConstant.admin.isConnected) {
            // load from database the limit to apply
            try {
                DbConfiguration configuration = new DbConfiguration(DbConstant.admin.session,
                        Configuration.configuration.HOST_ID);
                configuration.updateConfiguration();
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot load configuration from database", e);
            }
        } else {
            if (Configuration.configuration.baseDirectory != null &&
                    Configuration.configuration.configPath != null) {
                // load Rules from files
                File dirConfig = new File(Configuration.configuration.baseDirectory+
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
                            Configuration.configuration.baseDirectory+
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
     * @param document
     * @return True if OK
     */
    public static boolean loadDatabase(Document document) {
        Node node = document.selectSingleNode(XML_DBDRIVER);
        if (node == null) {
            logger.error("Unable to find DBDriver in Config file");
            DbConstant.admin = new DbAdmin(); //no database support
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
            Configuration.configuration.delayLimit = Long
                    .parseLong(node.getText());
            if (Configuration.configuration.delayLimit <= 0) {
                Configuration.configuration.delayLimit = 0;
            }
            logger.info("Delay Limit: {}",
                    Configuration.configuration.delayLimit);
        }
        node = document.selectSingleNode(XML_LIMITRUNNING);
        if (node != null) {
            Configuration.configuration.RUNNER_THREAD = Integer
                    .parseInt(node.getText());
            if (Configuration.configuration.RUNNER_THREAD < 10) {
                Configuration.configuration.RUNNER_THREAD = 10;
            }
            logger.info("Limit of Runner: {}",
                    Configuration.configuration.RUNNER_THREAD);
        }
        node = document.selectSingleNode(XML_DELAYCOMMANDER);
        if (node != null) {
            Configuration.configuration.delayCommander = Long
                    .parseLong(node.getText());
            if (Configuration.configuration.delayCommander <= 100) {
                Configuration.configuration.delayCommander = 100;
            }
            logger.info("Delay Commander: {}",
                    Configuration.configuration.delayCommander);
        }
        if (DbConstant.admin.isConnected) {
            node = document.selectSingleNode(XML_SERVER_HOSTID);
            Configuration.configuration.HOST_ID = node.getText();
            DbConfiguration configuration = new DbConfiguration(DbConstant.admin.session,
                    Configuration.configuration.HOST_ID,
                    Configuration.configuration.serverGlobalReadLimit,
                    Configuration.configuration.serverGlobalWriteLimit,
                    Configuration.configuration.serverChannelReadLimit,
                    Configuration.configuration.serverChannelWriteLimit,
                    Configuration.configuration.delayLimit);
            configuration.changeUpdatedInfo(UpdatedInfo.UPDATED);
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
