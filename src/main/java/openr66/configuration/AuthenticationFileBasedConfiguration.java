/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.configuration;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.xml.XmlDecl;
import goldengate.common.xml.XmlHash;
import goldengate.common.xml.XmlType;
import goldengate.common.xml.XmlUtil;
import goldengate.common.xml.XmlValue;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.data.DbHostAuth;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.DefaultElement;

/**
 * Authentication from File support
 *
 * @author Frederic Bregier
 *
 */
public class AuthenticationFileBasedConfiguration {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(AuthenticationFileBasedConfiguration.class);

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ROOT = "authent";
    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ENTRY = "entry";
    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_BASED = "/"+
        XML_AUTHENTIFICATION_ROOT+"/"+XML_AUTHENTIFICATION_ENTRY;

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
    private static final String XML_AUTHENTIFICATION_KEY = "key";

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ADMIN = "admin";

    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ADDRESS = "address";
    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_PORT = "port";
    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ISSSL = "isssl";
    /**
     * Authentication Fields
     */
    private static final String XML_AUTHENTIFICATION_ISCLIENT = "isclient";
    
    /**
     * Structure of the Configuration file
     *
     */
    private static final XmlDecl [] configAuthenticationDecls = {
        // identity
        new XmlDecl(XmlType.STRING, XML_AUTHENTIFICATION_HOSTID), 
        new XmlDecl(XmlType.STRING, XML_AUTHENTIFICATION_KEYFILE),
        new XmlDecl(XmlType.STRING, XML_AUTHENTIFICATION_KEY),
        new XmlDecl(XmlType.BOOLEAN, XML_AUTHENTIFICATION_ADMIN),
        new XmlDecl(XmlType.STRING, XML_AUTHENTIFICATION_ADDRESS),
        new XmlDecl(XmlType.INTEGER, XML_AUTHENTIFICATION_PORT),
        new XmlDecl(XmlType.BOOLEAN, XML_AUTHENTIFICATION_ISSSL),
        new XmlDecl(XmlType.BOOLEAN, XML_AUTHENTIFICATION_ISCLIENT)
    };
    /**
     * Global Structure for Server Configuration
     */
    private static final XmlDecl[] authentElements = {
        new XmlDecl(XML_AUTHENTIFICATION_ENTRY, XmlType.XVAL, XML_AUTHENTIFICATION_BASED, 
                configAuthenticationDecls, true)
    };
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
        XmlValue[] values = XmlUtil.read(document, authentElements);
        XmlHash hash = new XmlHash(values);
        XmlValue value = hash.get(XML_AUTHENTIFICATION_ENTRY);
        List<XmlValue[]> list = (List<XmlValue[]>) value.getList();
        Iterator<XmlValue[]> iterator = list.iterator();
        File key;
        byte[] byteKeys;
        while (iterator.hasNext()) {
            XmlValue [] subvalues = iterator.next();
            XmlHash subHash = new XmlHash(subvalues);
            value = subHash.get(XML_AUTHENTIFICATION_HOSTID);
            if (value == null || (value.isEmpty())) {
                continue;
            }
            String refHostId = value.getString();
            value = subHash.get(XML_AUTHENTIFICATION_KEYFILE);
            if (value == null || (value.isEmpty())) {
                value = subHash.get(XML_AUTHENTIFICATION_KEY);
                if (value == null || (value.isEmpty())) {
                    // Allow empty key
                    byteKeys = null;
                } else {
                    String skey = value.getString();
                    // key is crypted
                    if (skey.length() > 0) {
                        try {
                            byteKeys = Configuration.configuration.cryptoKey.decryptHexInBytes(skey);
                        } catch (Exception e) {
                            logger.error("Cannot read key for hostId " + refHostId+":"+skey);
                            continue;
                        }
                    } else {
                        byteKeys = null;
                    }
                }
            } else {
                String skey = value.getString();
                // load key from file
                key = new File(skey);
                if (!key.canRead()) {
                    logger.error("Cannot read key for hostId " + refHostId+":"+skey);
                    continue;
                }
                try {
                    byteKeys = Configuration.configuration.cryptoKey.decryptHexFile(key);
                } catch (Exception e2) {
                    logger.error("Cannot read key for hostId " + refHostId, e2);
                    continue;
                }
            }
            boolean isAdmin = false;
            value = subHash.get(XML_AUTHENTIFICATION_ADMIN);
            if (value != null && (!value.isEmpty())) {
                isAdmin = value.getBoolean();
            }
            value = subHash.get(XML_AUTHENTIFICATION_ADDRESS);
            if (value == null || (value.isEmpty())) {
                continue;
            }
            String address = value.getString();
            int port;
            value = subHash.get(XML_AUTHENTIFICATION_PORT);
            if (value != null && (!value.isEmpty())) {
                port = value.getInteger();
            } else {
                continue;
            }
            boolean isSsl = false;
            value = subHash.get(XML_AUTHENTIFICATION_ISSSL);
            if (value != null && (!value.isEmpty())) {
                isSsl = value.getBoolean();
            }
            boolean isClient = false;
            value = subHash.get(XML_AUTHENTIFICATION_ISCLIENT);
            if (value != null && (!value.isEmpty())) {
                isClient = value.getBoolean();
            }
            DbHostAuth auth = new DbHostAuth(DbConstant.admin.session,
                    refHostId, address, port, isSsl, byteKeys, isAdmin, isClient);
            try {
                if (auth.exist()) {
                    auth.update();
                } else {
                    auth.insert();
                }
            } catch (OpenR66DatabaseException e) {
                logger.error("Cannot create Authentication for hostId {}",refHostId);
                continue;
            }
            logger.debug("Add {} {}",refHostId,auth);
        }
        document = null;
        return true;
    }
    /**
     * Construct a new Element with value
     * @param name
     * @param value
     * @return the new Element
     */
    private static Element newElement(String name, String value) {
        Element node = new DefaultElement(name);
        node.addText(value);
        return node;
    }
    /**
     * Write all authentication to a file with filename
     * @param filename
     * @throws OpenR66ProtocolSystemException
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    public static void writeXML(String filename) throws OpenR66ProtocolSystemException, OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        Document document = DocumentHelper.createDocument();
        Element root = document.addElement(XML_AUTHENTIFICATION_ROOT);
        String request = "SELECT " +DbHostAuth.selectAllFields+" FROM "+DbHostAuth.table;
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement =
                new DbPreparedStatement(DbConstant.admin.session);
            preparedStatement.createPrepareStatement(request);
            preparedStatement.executeQuery();
            while (preparedStatement.getNext()) {
                DbHostAuth auth = DbHostAuth.getFromStatement(preparedStatement);
                Element entry = new DefaultElement(XML_AUTHENTIFICATION_ENTRY);
                entry.add(newElement(XML_AUTHENTIFICATION_HOSTID, auth.getHostid()));
                byte [] key = auth.getHostkey();
                String encode;
                try {
                    encode = Configuration.configuration.cryptoKey.cryptToHex(key);
                } catch (Exception e) {
                   encode = "";
                }
                entry.add(newElement(XML_AUTHENTIFICATION_KEY, encode));
                entry.add(newElement(XML_AUTHENTIFICATION_ADMIN, Boolean.toString(auth.isAdminrole())));
                entry.add(newElement(XML_AUTHENTIFICATION_ADDRESS, auth.getAddress()));
                entry.add(newElement(XML_AUTHENTIFICATION_PORT, Integer.toString(auth.getPort())));
                entry.add(newElement(XML_AUTHENTIFICATION_ISSSL, Boolean.toString(auth.isSsl())));
                root.add(entry);
            }
            FileUtils.writeXML(filename, null, document);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
    }
}
