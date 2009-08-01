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
package openr66.protocol.config;

import goldengate.common.digest.FilesystemBasedDigest;
import goldengate.common.digest.MD5;
import goldengate.common.file.DirInterface;
import goldengate.common.file.filesystembased.FilesystemBasedDirImpl;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdkAbstract;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import openr66.context.authentication.R66Auth;
import openr66.database.DbAdmin;
import openr66.database.DbConstant;
import openr66.database.data.DbR66HostAuth;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.model.DbModelFactory;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
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
public class R66FileBasedConfiguration {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66FileBasedConfiguration.class);

    /**
     * SERVER HOSTID
     */
    private static final String XML_SERVER_HOSTID = "/config/hostid";

    /**
     * SERVER PASSWORD (shutdown)
     */
    private static final String XML_SERVER_PASSWD = "/config/serverpasswd";

    /**
     * SERVER PORT
     */
    private static final String XML_SERVER_PORT = "/config/serverport";

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
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_BASED = "/authent/entry";

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_HOSTID = "hostid";

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_KEYFILE = "keyfile";

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ADMIN = "admin";

    /**
     *
     * @param document
     * @param fromXML
     * @return the new subpath
     * @throws OpenR66ProtocolSystemException
     */
    private String getSubPath(Document document, String fromXML)
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
     * Initiate the configuration from the xml file
     *
     * @param filename
     * @return True if OK
     */
    public boolean setConfigurationFromXml(String filename) {
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
        Node node = null;
        node = document.selectSingleNode(XML_SERVER_HOSTID);
        if (node == null) {
            logger.error("Unable to find Host ID in Config file: " + filename);
            return false;
        }
        Configuration.configuration.HOST_ID = node.getText();
        node = document.selectSingleNode(XML_SERVER_PASSWD);
        if (node == null) {
            logger.error("Unable to find Password in Config file: " + filename);
            return false;
        }
        String passwd = node.getText();
        // FIXME load from a file and store as a key
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
        node = document.selectSingleNode(XML_SERVER_PORT);
        int port = 21;
        if (node != null) {
            port = Integer.parseInt(node.getText());
        }
        Configuration.configuration.SERVER_PORT = port;
        node = document.selectSingleNode(XML_SERVER_HOME);
        if (node == null) {
            logger.error("Unable to find Home in Config file: " + filename);
            return false;
        }
        String path = node.getText();
        File file = new File(path);
        if (!file.isDirectory()) {
            logger.error("Home is not a directory in Config file: " + filename);
            return false;
        }
        try {
            Configuration.configuration.baseDirectory = FilesystemBasedDirImpl
                    .normalizePath(file.getCanonicalPath());
        } catch (IOException e1) {
            logger.error("Unable to set Home in Config file: " + filename);
            return false;
        }
        try {
            Configuration.configuration.configPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_CONFIGPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger
                    .error("Unable to set Config in Config file: " + filename,
                            e2);
            return false;
        }
        try {
            Configuration.configuration.inPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_INPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set In in Config file: " + filename, e2);
            return false;
        }
        try {
            Configuration.configuration.outPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_OUTPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Out in Config file: " + filename, e2);
            return false;
        }
        try {
            Configuration.configuration.workingPath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_WORKINGPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Working in Config file: " + filename,
                    e2);
            return false;
        }
        try {
            Configuration.configuration.archivePath = FilesystemBasedDirImpl
                    .normalizePath(getSubPath(document, XML_ARCHIVEPATH));
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Archive in Config file: " + filename,
                    e2);
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
        // FIXME should be removed and set from database
        node = document.selectSingleNode(XML_LIMITGLOBAL);
        if (node != null) {
            Configuration.configuration.serverGlobalReadLimit = Long
                    .parseLong(node.getText());
            if (Configuration.configuration.serverGlobalReadLimit <= 0) {
                Configuration.configuration.serverGlobalReadLimit = 0;
            }
            Configuration.configuration.serverGlobalWriteLimit = Configuration.configuration.serverGlobalReadLimit;
            logger.warn("Global Limit: {}",
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
            logger.warn("SessionInterface Limit: {}",
                    Configuration.configuration.serverChannelReadLimit);
        }
        Configuration.configuration.delayLimit = AbstractTrafficShapingHandler.DEFAULT_CHECK_INTERVAL;
        node = document.selectSingleNode(XML_TIMEOUTCON);
        if (node != null) {
            Configuration.configuration.TIMEOUTCON = Integer.parseInt(node
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
                        logger.info("FastMD5 init lib to " +
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
        node = document.selectSingleNode(XML_DBDRIVER);
        if (node == null) {
            logger.error("Unable to find DBDriver in Config file: " + filename);
            //return false;
            DbConstant.admin = new DbAdmin(); //no database support
            // load Rules from files
            File dirConfig = new File(Configuration.configuration.baseDirectory+
                    Configuration.configuration.configPath);
            if (dirConfig.isDirectory()) {
                try {
                    R66RuleFileBasedConfiguration.importRules(dirConfig);
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
        } else {
            String dbdriver = node.getText();
            node = document.selectSingleNode(XML_DBSERVER);
            if (node == null) {
                logger.error("Unable to find DBServer in Config file: " + filename);
                return false;
            }
            String dbserver = node.getText();
            node = document.selectSingleNode(XML_DBUSER);
            if (node == null) {
                logger.error("Unable to find DBUser in Config file: " + filename);
                return false;
            }
            String dbuser = node.getText();
            node = document.selectSingleNode(XML_DBPASSWD);
            if (node == null) {
                logger.error("Unable to find DBPassword in Config file: " +
                        filename);
                return false;
            }
            String dbpasswd = node.getText();
            if (dbdriver == null || dbserver == null || dbuser == null ||
                    dbpasswd == null || dbdriver.length() == 0 ||
                    dbserver.length() == 0 || dbuser.length() == 0 ||
                    dbpasswd.length() == 0) {
                logger.error("Unable to find Correct DB data in Config file: " +
                        filename);
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
        // We use Apache Commons IO
        FilesystemBasedDirJdkAbstract.ueApacheCommonsIo = true;

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
                if (! loadAuthentication(fileauthent)) {
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
        return true;
    }
    /**
     * Load Authentication from File
     * @param filename
     * @return True if OK
     */
    @SuppressWarnings("unchecked")
    public static boolean loadAuthentication(String filename) {
        Document document = null;
        try {
            document = new SAXReader().read(filename);
        } catch (DocumentException e) {
            logger.error("Unable to read the XML Authentication file: " +
                    filename, e);
            return false;
        }
        if (document == null) {
            logger.error("Unable to read the XML Authentication file: " +
                    filename);
            return false;
        }
        List<Node> list = document.selectNodes(XML_AUTHENTIFICATION_BASED);
        Iterator<Node> iterator = list.iterator();
        Node nodebase, node;
        File key;
        byte[] byteKeys;
        FileInputStream inputStream = null;
        while (iterator.hasNext()) {
            nodebase = iterator.next();
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_HOSTID);
            if (node == null) {
                continue;
            }
            String refHostId = node.getText();
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_KEYFILE);
            if (node == null) {
                continue;
            }
            String skey = node.getText();
            // FIXME load key from file
            key = new File(skey);
            if (!key.canRead()) {
                logger.warn("Cannot read key for hostId " + refHostId);
                continue;
            }
            byteKeys = new byte[(int) key.length()];
            try {
                inputStream = new FileInputStream(key);
                inputStream.read(byteKeys);
                inputStream.close();
            } catch (IOException e) {
                logger.warn("Cannot read key for hostId " + refHostId, e);
                try {
                    if (inputStream != null)
                        inputStream.close();
                } catch (IOException e1) {
                }
                continue;
            }
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_ADMIN);
            boolean isAdmin = false;
            if (node != null) {
                isAdmin = node.getText().equals("1")? true : false;
            }
            DbR66HostAuth auth = new DbR66HostAuth(DbConstant.admin.session,
                    refHostId, byteKeys, isAdmin);
            try {
                if (auth.exist()) {
                    auth.update();
                } else {
                    auth.insert();
                }
            } catch (OpenR66DatabaseException e) {
                logger.warn("Cannot create Authentication for hostId " + refHostId);
                continue;
            }
            logger.info("Add " + refHostId + " " + auth.toString());
        }
        document = null;
        return true;
    }
}
