/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.configuration;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.data.DbHostAuth;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

import org.apache.commons.codec.binary.Base64;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
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
                node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_KEY);
                if (node == null) {
                    continue;
                }
                String skey = node.getText();
                // FIXME key is coded with Base64 algorithm
                byteKeys = Base64.decodeBase64(skey.getBytes());
            } else {
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
            }
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_ADMIN);
            boolean isAdmin = false;
            if (node != null) {
                isAdmin = node.getText().equals("1")? true : false;
                if (! isAdmin) {
                    isAdmin = node.getText().equalsIgnoreCase("true")? true : false;
                }
            }
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_ADDRESS);
            if (node == null) {
                continue;
            }
            String address = node.getText();
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_PORT);
            if (node == null) {
                continue;
            }
            int port = Integer.parseInt(node.getText());
            node = nodebase.selectSingleNode(XML_AUTHENTIFICATION_ISSSL);
            boolean isSsl = false;
            if (node != null) {
                isSsl = node.getText().equals("1")? true : false;
                if (! isSsl) {
                    isSsl = node.getText().equalsIgnoreCase("true")? true : false;
                }
            }
            DbHostAuth auth = new DbHostAuth(DbConstant.admin.session,
                    refHostId, address, port, isSsl, byteKeys, isAdmin);
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
                byte [] encode = Base64.encodeBase64(key);
                entry.add(newElement(XML_AUTHENTIFICATION_KEY, new String(encode)));
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
