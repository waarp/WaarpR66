package org.waarp.openr66.dao.xml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class XMLHostDAO implements HostDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(XMLHostDAO.class);

    public static final String HOSTID_FIELD = "hostid";
    public static final String ADDRESS_FIELD = "address";
    public static final String PORT_FIELD = "port";
    public static final String IS_SSL_FIELD = "isssl";
    public static final String IS_CLIENT_FIELD = "isclient";
    public static final String IS_ACTIVE_FIELD = "isactive";
    public static final String IS_PROXIFIED_FIELD = "isproxified";
    public static final String HOSTKEY_FIELD = "key";
    public static final String ADMINROLE_FIELD = "adminrole";

    private static final String XML_SELECT = "/authent/entry[hostid=$hostid]";
    private static final String XML_GET_ALL= "/authent/entry";

    private File file;

    public XMLHostDAO(String filePath) {
        this.file = new File(filePath);
    }

    public void close() {}

    public void delete(Host host) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public void deleteAll() throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public List<Host> getAll() throws DAOException {
        if (!file.exists()) {
            throw new DAOException("File doesn't exist");
        }
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            Document document = dbf.newDocumentBuilder().parse(file);
            // Setup XPath query
            XPath xPath = XPathFactory.newInstance().newXPath();
            XPathExpression xpe = xPath.compile(XML_GET_ALL);
            NodeList listNode = (NodeList) xpe.evaluate(document,
                    XPathConstants.NODESET);
            // Iterate through all found nodes
            List<Host> res = new ArrayList<Host>(listNode.getLength());
            for (int i = 0; i < listNode.getLength(); i++) {
                Node node = listNode.item(i);
                res.add(getFromNode(node));
            }
            return res;
        } catch (SAXException e) {
            throw new DAOException(e);
        } catch (XPathExpressionException e) {
            throw new DAOException(e);
        } catch (ParserConfigurationException e) {
            throw new DAOException(e);
        } catch (IOException e) {
            throw new DAOException(e);
        }
    }

    public boolean exist(String hostid) throws DAOException {
        if (!file.exists()) {
            throw new DAOException("File doesn't exist");
        }
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            Document document = dbf.newDocumentBuilder().parse(file);
            // Setup XPath variable
            SimpleVariableResolver resolver = new SimpleVariableResolver();
            resolver.addVariable(new QName(null, "hostid"), hostid);
            // Setup XPath query
            XPath xPath = XPathFactory.newInstance().newXPath();
            xPath.setXPathVariableResolver(resolver);
            XPathExpression xpe = xPath.compile(XML_SELECT);
            // Query will return "" if nothing is found
            return(!"".equals(xpe.evaluate(document)));
        } catch (SAXException e) {
            throw new DAOException(e);
        } catch (XPathExpressionException e) {
            throw new DAOException(e);
        } catch (ParserConfigurationException e) {
            throw new DAOException(e);
        } catch (IOException e) {
            throw new DAOException(e);
        }
    }

    public List<Host> find(List<Filter> fitlers) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public void insert(Host host) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public Host select(String hostid) throws DAOException {
        if (!file.exists()) {
            throw new DAOException("File " + file.getPath() + " doesn't exist");
        }
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            Document document = dbf.newDocumentBuilder().parse(file);
            // Setup XPath variable
            SimpleVariableResolver resolver = new SimpleVariableResolver();
            resolver.addVariable(new QName(null, "hostid"), hostid);
            // Setup XPath query
            XPath xPath = XPathFactory.newInstance().newXPath();
            xPath.setXPathVariableResolver(resolver);
            XPathExpression xpe = xPath.compile(XML_SELECT);
            // Retrieve node and instantiate object
            Node node = (Node) xpe.evaluate(document, XPathConstants.NODE);
            if (node != null) {
                 return getFromNode(node);
            }
            return null;
        } catch (SAXException e) {
            throw new DAOException(e);
        } catch (XPathExpressionException e) {
            throw new DAOException(e);
        } catch (ParserConfigurationException e) {
            throw new DAOException(e);
        } catch (IOException e) {
            throw new DAOException(e);
        }
    }

    public void update(Host host) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    private Host getFromNode(Node parent) {
        Host res = new Host();

        NodeList children = parent.getChildNodes();
        for (int j = 0; j < children.getLength(); j++) {
            Node node = children.item(j);
            if (node.getNodeName().equals(HOSTID_FIELD)) {
                res.setHostid(node.getTextContent());
            } else if (node.getNodeName().equals(ADDRESS_FIELD)) {
                res.setAddress(node.getTextContent());
            } else if (node.getNodeName().equals(PORT_FIELD)) {
                res.setPort(Integer.parseInt(node.getTextContent()));
            } else if (node.getNodeName().equals(IS_SSL_FIELD)) {
                res.setSSL(Boolean.parseBoolean(node.getTextContent()));
            } else if (node.getNodeName().equals(IS_CLIENT_FIELD)) {
                res.setClient(Boolean.parseBoolean(node.getTextContent()));
            } else if (node.getNodeName().equals(IS_PROXIFIED_FIELD)) {
                res.setProxified(Boolean.parseBoolean(node.getTextContent()));
            } else if (node.getNodeName().equals(ADMINROLE_FIELD)) {
                res.setAdmin(Boolean.parseBoolean(node.getTextContent()));
            } else if (node.getNodeName().equals(HOSTKEY_FIELD)) {
                res.setHostkey(node.getTextContent().getBytes());
            }
        }
        return res;
    }

    private Node getNode(Document doc, Host host) {
        Node res = doc.createElement("entry");
        res.appendChild(XMLUtils.createNode(doc, HOSTID_FIELD,
                host.getHostid()));
        res.appendChild(XMLUtils.createNode(doc, ADDRESS_FIELD,
                host.getAddress()));
        res.appendChild(XMLUtils.createNode(doc, PORT_FIELD,
                Integer.toString(host.getPort())));
        res.appendChild(XMLUtils.createNode(doc, IS_SSL_FIELD,
                Boolean.toString(host.isSSL())));
        res.appendChild(XMLUtils.createNode(doc, IS_CLIENT_FIELD,
                Boolean.toString(host.isClient())));
        res.appendChild(XMLUtils.createNode(doc, IS_PROXIFIED_FIELD,
                Boolean.toString(host.isProxified())));
        res.appendChild(XMLUtils.createNode(doc, ADMINROLE_FIELD,
                Boolean.toString(host.isAdmin())));
        try {
            res.appendChild(XMLUtils.createNode(doc, HOSTKEY_FIELD,
                    new String(host.getHostkey(), "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported charset ! Should not happened");
        }
        return res;
    }
}
