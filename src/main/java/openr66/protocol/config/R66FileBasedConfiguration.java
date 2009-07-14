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
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.file.filesystembased.specific.FilesystemBasedDirJdkAbstract;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import openr66.authentication.R66SimpleAuth;
import openr66.filesystem.R66Dir;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.jboss.netty.handler.traffic.AbstractTrafficShapingHandler;

/**
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
     * All authentications
     */
    private final ConcurrentHashMap<String, R66SimpleAuth> authentications = new ConcurrentHashMap<String, R66SimpleAuth>();

    /**
     *
     * @param document
     * @param fromXML
     * @return the new subpath as a File
     * @throws OpenR66ProtocolSystemException
     */
    private File getSubPath(Document document, String fromXML) throws OpenR66ProtocolSystemException {
        Node node = document.selectSingleNode(fromXML);
        if (node == null) {
            logger.error("Unable to find CONFIG Path in Config file");
            throw new OpenR66ProtocolSystemException("Unable to find a Path in Config file: "+fromXML);
        }
        String path = node.getText();
        String newpath = FileUtils.consolidatePath(Configuration.configuration.baseDirectory, path);
        File file = new File(newpath);
        if (!file.isDirectory()) {
            FileUtils.createDir(file);
        }
        return file;
    }
    /**
     * Initiate the configuration from the xml file
     *
     * @param filename
     * @return True if OK
     */
    @SuppressWarnings("unchecked")
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
        Node nodebase, node = null;
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
        if (! key.canRead()) {
            logger.error("Unable to Load Server Password in Config file from: " + passwd);
            return false;
        }
        byte [] byteKeys = new byte[(int)key.length()];
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(key);
            inputStream.read(byteKeys);
            inputStream.close();
        } catch (IOException e2) {
            logger.error("Unable to Load Server Password in Config file from: " + passwd, e2);
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
            Configuration.configuration.baseDirectory = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (IOException e1) {
            logger.error("Unable to set Home in Config file: " + filename);
            return false;
        }
        try {
            file = getSubPath(document, XML_CONFIGPATH);
            Configuration.configuration.configPath = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Config in Config file: " + filename, e2);
            return false;
        } catch (IOException e1) {
            logger.error("Unable to set Config in Config file: " + filename, e1);
            return false;
        }
        // Get the rules
        try {
            R66RuleFileBasedConfiguration.importRules(file);
        } catch (OpenR66ProtocolSystemException e3) {
            logger.error("Unable to load Rules from Config dir: " +
                    Configuration.configuration.configPath, e3);
            return false;
        }

        try {
            file = getSubPath(document, XML_INPATH);
            Configuration.configuration.inPath = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set In in Config file: " + filename, e2);
            return false;
        } catch (IOException e1) {
            logger.error("Unable to set In in Config file: " + filename, e1);
            return false;
        }
        try {
            file = getSubPath(document, XML_OUTPATH);
            Configuration.configuration.outPath = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Out in Config file: " + filename, e2);
            return false;
        } catch (IOException e1) {
            logger.error("Unable to set Out in Config file: " + filename, e1);
            return false;
        }
        try {
            file = getSubPath(document, XML_WORKINGPATH);
            Configuration.configuration.workingPath = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Working in Config file: " + filename, e2);
            return false;
        } catch (IOException e1) {
            logger.error("Unable to set Working in Config file: " + filename, e1);
            return false;
        }
        try {
            file = getSubPath(document, XML_ARCHIVEPATH);
            Configuration.configuration.archivePath = R66Dir.normalizePath(file
                    .getCanonicalPath());
        } catch (OpenR66ProtocolSystemException e2) {
            logger.error("Unable to set Archive in Config file: " + filename, e2);
            return false;
        } catch (IOException e1) {
            logger.error("Unable to set Archive in Config file: " + filename, e1);
            return false;
        }

        node = document.selectSingleNode(XML_SERVER_THREAD);
        if (node != null) {
            Configuration.configuration.SERVER_THREAD =
                Integer.parseInt(node.getText());
        }
        node = document.selectSingleNode(XML_CLIENT_THREAD);
        if (node != null) {
            Configuration.configuration.CLIENT_THREAD =
                Integer.parseInt(node.getText());
        }
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
            Configuration.configuration.TIMEOUTCON = Integer.parseInt(node.getText());
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
            Configuration.configuration.BLOCKSIZE = Integer.parseInt(node.getText());
        }
        // We use Apache Commons IO
        FilesystemBasedDirJdkAbstract.ueApacheCommonsIo = true;
        node = document.selectSingleNode(XML_AUTHENTIFICATION_FILE);
        if (node == null) {
            logger.error("Unable to find Authentication file in Config file: " +
                    filename);
            return false;
        }
        String fileauthent = node.getText();
        document = null;
        try {
            document = new SAXReader().read(fileauthent);
        } catch (DocumentException e) {
            logger.error("Unable to read the XML Authentication file: " +
                    fileauthent, e);
            return false;
        }
        if (document == null) {
            logger.error("Unable to read the XML Authentication file: " +
                    fileauthent);
            return false;
        }
        List<Node> list = document.selectNodes(XML_AUTHENTIFICATION_BASED);
        Iterator<Node> iterator = list.iterator();
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
            if (! key.canRead()) {
                logger.warn("Cannot read key for hostId "+refHostId);
                continue;
            }
            byteKeys = new byte[(int) key.length()];
            try {
                inputStream = new FileInputStream(key);
                inputStream.read(byteKeys);
                inputStream.close();
            } catch (IOException e) {
                logger.warn("Cannot read key for hostId "+refHostId, e);
                try {
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
            R66SimpleAuth auth = new R66SimpleAuth(refHostId, byteKeys);
            auth.setAdmin(isAdmin);
            authentications.put(refHostId, auth);
            logger.warn("Add "+refHostId+" "+auth.toString());
        }
        document = null;
        return true;
    }

    /**
     * @param user
     * @return the SimpleAuth if any for this user
     */
    public R66SimpleAuth getSimpleAuth(String user) {
        return authentications.get(user);
    }
}
