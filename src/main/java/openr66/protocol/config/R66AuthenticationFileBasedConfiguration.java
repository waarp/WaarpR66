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
package openr66.protocol.config;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import openr66.database.DbConstant;
import openr66.database.data.DbR66HostAuth;
import openr66.database.exception.OpenR66DatabaseException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * Authentication from File support
 *
 * @author Frederic Bregier
 *
 */
public class R66AuthenticationFileBasedConfiguration {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66AuthenticationFileBasedConfiguration.class);

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
