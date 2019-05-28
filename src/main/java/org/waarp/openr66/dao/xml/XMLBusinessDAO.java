package org.waarp.openr66.dao.xml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Business;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//TODO
public class XMLBusinessDAO implements BusinessDAO {

    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(XMLBusinessDAO.class);

    public static final String HOSTID_FIELD = "hostid";

    private static final String XML_SELECT = "/authent/entry[hostid=$hostid]";
    private static final String XML_GET_ALL= "/authent/entry";

    private File file;

    public XMLBusinessDAO(String filePath) throws DAOException {
        this.file = new File(filePath);
    }

    public void close() {}

    public void delete(Business business) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public void deleteAll() throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public List<Business> getAll() throws DAOException {
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
            List<Business> res = new ArrayList<Business>(listNode.getLength());
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

    public List<Business> find(List<Filter> fitlers) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public void insert(Business business) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    public Business select(String hostid) throws DAOException {
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

    public void update(Business business) throws DAOException {
        throw new DAOException("Operation not supported on XML DAO");
    }

    private Business getFromNode(Node parent) {
        Business res = new Business();

        NodeList children = parent.getChildNodes();
        for (int j = 0; j < children.getLength(); j++) {
            Node node = children.item(j);
            if (node.getNodeName().equals(HOSTID_FIELD)) {
                res.setHostid(node.getTextContent());
            }
        }
        return res;
    }

    private Node getNode(Document doc, Business business) {
        Node res = doc.createElement("entry");
        return res;
    }
}
