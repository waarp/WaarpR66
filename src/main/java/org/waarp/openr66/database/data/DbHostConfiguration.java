/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.database.data;

import java.io.StringReader;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.data.DbValue;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseNoDataException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.role.RoleDefault;
import org.waarp.common.role.RoleDefault.ROLE;
import org.waarp.common.xml.XmlDecl;
import org.waarp.common.xml.XmlHash;
import org.waarp.common.xml.XmlType;
import org.waarp.common.xml.XmlUtil;
import org.waarp.common.xml.XmlValue;
import org.waarp.openr66.commander.CommanderNoDb;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.utils.Version;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Configuration Table object
 * 
 * @author Frederic Bregier
 * 
 */
public class DbHostConfiguration extends AbstractDbData {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(DbHostConfiguration.class);

    public static enum Columns {
        BUSINESS,
        ROLES,
        ALIASES,
        OTHERS,
        UPDATEDINFO,
        HOSTID
    }

    public static final int[] dbTypes = {
            Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR, Types.LONGVARCHAR,
            Types.INTEGER, Types.NVARCHAR };

    public static final String table = " HOSTCONFIG ";

    /**
     * HashTable in case of lack of database
     */
    private static final ConcurrentHashMap<String, DbHostConfiguration> dbR66HostConfigurationHashMap =
            new ConcurrentHashMap<String, DbHostConfiguration>();

    private String hostid;

    private String business;

    private String roles;

    private String aliases;

    private String others;

    private int updatedInfo = UpdatedInfo.UNKNOWN
            .ordinal();

    public static final String XML_ALIASES = "aliases";

    public static final String XML_ROLES = "roles";

    public static final String XML_BUSINESS = "business";

    /**
     * Alias Id
     */
    public static final String XML_ALIASID = "aliasid";

    /**
     * Main ID in alias
     */
    public static final String XML_REALID = "realid";

    /**
     * Alias
     */
    public static final String XML_ALIAS = "alias";

    /**
     * Role set
     */
    public static final String XML_ROLESET = "roleset";

    /**
     * ID in role
     */
    public static final String XML_ROLEID = "roleid";

    /**
     * Role Main entry
     */
    public static final String XML_ROLE = "role";

    /**
     * Check version in protocol
     */
    public static final String XML_BUSINESSID = "businessid";
    private static final XmlDecl[] businessDecl = { new XmlDecl(XML_BUSINESS, XmlType.STRING, XML_BUSINESS + "/"
            + XML_BUSINESSID, true) };
    /**
     * Structure of the Configuration file
     * 
     */
    public static final XmlDecl[] configRoleDecls = {
            // roles
            new XmlDecl(XmlType.STRING, XML_ROLEID),
            new XmlDecl(XmlType.STRING, XML_ROLESET)
    };
    private static final XmlDecl[] roleDecl = { new XmlDecl(XML_ROLES, XmlType.XVAL, XML_ROLES + "/"
            + XML_ROLE, configRoleDecls, true) };
    /**
     * Structure of the Configuration file
     * 
     */
    public static final XmlDecl[] configAliasDecls = {
            // alias
            new XmlDecl(XmlType.STRING, XML_REALID),
            new XmlDecl(XmlType.STRING, XML_ALIASID)
    };
    private static final XmlDecl[] aliasDecl = { new XmlDecl(XML_ALIASES, XmlType.XVAL, XML_ALIASES + "/"
            + XML_ALIAS, configAliasDecls, true) };

    public static enum OtherFields {
        root, version, seeallid
    };

    // ALL TABLE SHOULD IMPLEMENT THIS
    public static final int NBPRKEY = 1;

    protected static final String selectAllFields = Columns.BUSINESS
            .name()
            +
            ","
            +
            Columns.ROLES
                    .name()
            +
            ","
            +
            Columns.ALIASES
                    .name()
            +
            ","
            +
            Columns.OTHERS
                    .name()
            +
            ","
            + Columns.UPDATEDINFO
                    .name()
            + ","
            + Columns.HOSTID
                    .name();

    protected static final String updateAllFields = Columns.BUSINESS
            .name()
            +
            "=?,"
            +
            Columns.ROLES
                    .name()
            +
            "=?,"
            +
            Columns.ALIASES
                    .name()
            +
            "=?,"
            +
            Columns.OTHERS
                    .name()
            +
            "=?,"
            +
            Columns.UPDATEDINFO
                    .name()
            +
            "=?";

    protected static final String insertAllValues = " (?,?,?,?,?,?) ";

    @Override
    protected void initObject() {
        primaryKey = new DbValue[] { new DbValue(hostid, Columns.HOSTID
                .name()) };
        otherFields = new DbValue[] {
                new DbValue(business, Columns.BUSINESS.name(), true),
                new DbValue(roles, Columns.ROLES.name(), true),
                new DbValue(aliases, Columns.ALIASES.name(), true),
                new DbValue(others, Columns.OTHERS.name(), true),
                new DbValue(updatedInfo, Columns.UPDATEDINFO.name()) };
        allFields = new DbValue[] {
                otherFields[0], otherFields[1], otherFields[2], otherFields[3],
                otherFields[4], primaryKey[0] };
    }

    @Override
    protected String getSelectAllFields() {
        return selectAllFields;
    }

    @Override
    protected String getTable() {
        return table;
    }

    @Override
    protected String getInsertAllValues() {
        return insertAllValues;
    }

    @Override
    protected String getUpdateAllFields() {
        return updateAllFields;
    }

    @Override
    protected void setToArray() {
        allFields[Columns.HOSTID.ordinal()].setValue(hostid);
        if (business == null) {
            business = "";
        } else {
            int len;
            do {
                len = business.length();
                business = business.replaceAll("\\s+", " ");
            } while (len != business.length());
        }
        allFields[Columns.BUSINESS.ordinal()].setValue(business);
        if (roles == null) {
            roles = "";
        } else {
            int len;
            do {
                len = roles.length();
                roles = roles.replaceAll("\\s+", " ");
            } while (len != roles.length());
        }
        allFields[Columns.ROLES.ordinal()]
                .setValue(roles);
        if (aliases == null) {
            aliases = "";
        } else {
            int len;
            do {
                len = aliases.length();
                aliases = aliases.replaceAll("\\s+", " ");
            } while (len != aliases.length());
        }
        allFields[Columns.ALIASES.ordinal()]
                .setValue(aliases);
        if (others == null) {
            others = "";
        } else {
            int len;
            do {
                len = others.length();
                others = others.replaceAll("\\s+", " ");
            } while (len != others.length());
        }
        allFields[Columns.OTHERS.ordinal()]
                .setValue(others);
        allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
    }

    @Override
    protected void setFromArray() throws WaarpDatabaseSqlException {
        hostid = (String) allFields[Columns.HOSTID.ordinal()].getValue();
        business = (String) allFields[Columns.BUSINESS.ordinal()].getValue();
        roles = (String) allFields[Columns.ROLES.ordinal()].getValue();
        aliases = (String) allFields[Columns.ALIASES.ordinal()].getValue();
        others = (String) allFields[Columns.OTHERS.ordinal()].getValue();
        updatedInfo = (Integer) allFields[Columns.UPDATEDINFO.ordinal()]
                .getValue();
    }

    @Override
    protected String getWherePrimaryKey() {
        return primaryKey[0].getColumn() + " = ? ";
    }

    @Override
    protected void setPrimaryKey() {
        primaryKey[0].setValue(hostid);
    }

    /**
     * @param dbSession
     * @param hostid
     * @param business
     *            Business configuration
     * @param roles
     *            Roles configuration
     * @param aliases
     *            Aliases configuration
     * @param others
     *            Other configuration
     */
    public DbHostConfiguration(DbSession dbSession, String hostid, String business, String roles, String aliases,
            String others) {
        super(dbSession);
        this.hostid = hostid;
        this.business = business;
        this.roles = roles;
        this.aliases = aliases;
        this.others = others;
        setToArray();
        isSaved = false;
    }

    /**
     * Constructor from Json
     * 
     * @param dbSession
     * @param source
     * @throws WaarpDatabaseSqlException
     */
    public DbHostConfiguration(DbSession dbSession, ObjectNode source) throws WaarpDatabaseSqlException {
        super(dbSession);
        setFromJson(source, false);
        if (hostid == null || hostid.isEmpty()) {
            throw new WaarpDatabaseSqlException("Not enough argument to create the object");
        }
        setToArray();
        isSaved = false;
    }

    /**
     * @param dbSession
     * @param hostid
     * @throws WaarpDatabaseException
     */
    public DbHostConfiguration(DbSession dbSession, String hostid) throws WaarpDatabaseException {
        super(dbSession);
        this.hostid = hostid;
        // load from DB
        select();
    }

    /**
     * @return the hostid
     */
    public String getHostid() {
        return hostid;
    }

    /**
     * @return the business
     */
    public String getBusiness() {
        return business;
    }

    /**
     * @param business
     *            the business to set
     */
    public void setBusiness(String business) {
        this.business = business == null ? "" : business;
        int len;
        do {
            len = this.business.length();
            this.business = this.business.replaceAll("\\s+", " ");
        } while (len != this.business.length());
        Configuration.configuration.getBusinessWhiteSet().clear();
        if (!this.business.isEmpty()) {
            readValuesFromXml(this.business, businessDecl);
        }
        allFields[Columns.BUSINESS.ordinal()].setValue(business);
        isSaved = false;
    }

    /**
     * @return the roles
     */
    public String getRoles() {
        return roles;
    }

    /**
     * @param roles
     *            the roles to set
     */
    public void setRoles(String roles) {
        this.roles = roles == null ? "" : roles;
        int len;
        do {
            len = this.roles.length();
            this.roles = this.roles.replaceAll("\\s+", " ");
        } while (len != this.roles.length());
        Configuration.configuration.getRoles().clear();
        if (!this.roles.isEmpty()) {
            readValuesFromXml(this.roles, roleDecl);
        }
        allFields[Columns.ROLES.ordinal()].setValue(roles);
        isSaved = false;
    }

    /**
     * @return the aliases
     */
    public String getAliases() {
        return aliases;
    }

    /**
     * @param aliases
     *            the aliases to set
     */
    public void setAliases(String aliases) {
        this.aliases = aliases == null ? "" : aliases;
        int len;
        do {
            len = this.aliases.length();
            this.aliases = this.aliases.replaceAll("\\s+", " ");
        } while (len != this.aliases.length());
        Configuration.configuration.getAliases().clear();
        Configuration.configuration.getReverseAliases().clear();
        if (!this.aliases.isEmpty()) {
            readValuesFromXml(this.aliases, aliasDecl);
        }
        allFields[Columns.ALIASES.ordinal()].setValue(aliases);
        isSaved = false;
    }

    @SuppressWarnings("unchecked")
    private void readValuesFromXml(String input, XmlDecl[] config) {
        Document document = null;
        StringReader reader = new StringReader(input);
        // Open config file
        try {
            document = new SAXReader().read(reader);
        } catch (DocumentException e) {
            logger.error(Messages.getString("FileBasedConfiguration.CannotReadXml") + input, e); //$NON-NLS-1$
            return;
        }
        if (document == null) {
            logger.error(Messages.getString("FileBasedConfiguration.CannotReadXml") + input); //$NON-NLS-1$
            return;
        }
        XmlValue[] configuration = XmlUtil.read(document, config);
        XmlHash hashConfig = new XmlHash(configuration);
        XmlValue value = hashConfig.get(DbHostConfiguration.XML_BUSINESS);
        if (value != null && (value.getList() != null)) {
            List<String> ids = (List<String>) value.getList();
            if (ids != null) {
                for (String sval : ids) {
                    if (sval.isEmpty()) {
                        continue;
                    }
                    logger.info("Business Allow: " + sval);
                    Configuration.configuration.getBusinessWhiteSet().add(sval.trim());
                }
                ids.clear();
                ids = null;
            }
        }
        value = hashConfig.get(DbHostConfiguration.XML_ALIASES);
        if (value != null && (value.getList() != null)) {
            for (XmlValue[] xml : (List<XmlValue[]>) value.getList()) {
                XmlHash subHash = new XmlHash(xml);
                value = subHash.get(DbHostConfiguration.XML_REALID);
                if (value == null || (value.isEmpty())) {
                    continue;
                }
                String refHostId = value.getString();
                value = subHash.get(DbHostConfiguration.XML_ALIASID);
                if (value == null || (value.isEmpty())) {
                    continue;
                }
                String aliasset = value.getString();
                String[] alias = aliasset.split(" |\\|");
                for (String namealias : alias) {
                    Configuration.configuration.getAliases().put(namealias, refHostId);
                }
                Configuration.configuration.getReverseAliases().put(refHostId, alias);
                logger.info("Aliases for: " + refHostId + " = " + aliasset);
            }
        }
        value = hashConfig.get(DbHostConfiguration.XML_ROLES);
        if (value != null && (value.getList() != null)) {
            for (XmlValue[] xml : (List<XmlValue[]>) value.getList()) {
                XmlHash subHash = new XmlHash(xml);
                value = subHash.get(DbHostConfiguration.XML_ROLEID);
                if (value == null || (value.isEmpty())) {
                    continue;
                }
                String refHostId = value.getString();
                value = subHash.get(DbHostConfiguration.XML_ROLESET);
                if (value == null || (value.isEmpty())) {
                    continue;
                }
                String roleset = value.getString();
                String[] roles = roleset.split(" |\\|");
                RoleDefault newrole = new RoleDefault();
                for (String role : roles) {
                    try {
                        RoleDefault.ROLE roletype = RoleDefault.ROLE.valueOf(role.toUpperCase());
                        if (roletype == ROLE.NOACCESS) {
                            // reset
                            newrole.setRole(roletype);
                        } else {
                            newrole.addRole(roletype);
                        }
                    } catch (IllegalArgumentException e) {
                        // ignore
                    }
                }
                logger.info("New Role: " + refHostId + ":" + newrole);
                Configuration.configuration.getRoles().put(refHostId, newrole);
            }
        }
        hashConfig.clear();
        hashConfig = null;
        configuration = null;
    }

    /**
     * @return the others
     */
    public String getOthers() {
        return others;
    }

    /**
     * @param others
     *            the others to set
     */
    public void setOthers(String others) {
        this.others = others == null ? "" : others;
        int len;
        do {
            len = this.others.length();
            this.others = this.others.replaceAll("\\s+", " ");
        } while (len != this.others.length());
        allFields[Columns.OTHERS.ordinal()].setValue(others);
        isSaved = false;
    }

    /**
     * 
     * @return the element for the content of the other part
     */
    public Element getOtherElement() {
        if (others != null && !others.isEmpty()) {
            Document document;
            try {
                document = DocumentHelper.parseText(others);
            } catch (DocumentException e) {
                return DocumentHelper.createElement(OtherFields.root.name());
            }
            return document.getRootElement();
        } else {
            return DocumentHelper.createElement(OtherFields.root.name());
        }
    }

    /**
     * 
     * @param element
     *            the element to set as XML string to other part
     */
    public void setOtherElement(Element element) {
        setOthers(element.asXML());
    }

    @Override
    public void delete() throws WaarpDatabaseException {
        if (dbSession == null) {
            dbR66HostConfigurationHashMap.remove(this.hostid);
            isSaved = false;
            return;
        }
        super.delete();
    }

    @Override
    public void insert() throws WaarpDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66HostConfigurationHashMap.put(this.hostid, this);
            isSaved = true;
            if (this.updatedInfo == UpdatedInfo.TOSUBMIT.ordinal()) {
                CommanderNoDb.todoList.add(this);
            }
            return;
        }
        super.insert();
    }

    @Override
    public boolean exist() throws WaarpDatabaseException {
        if (dbSession == null) {
            return dbR66HostConfigurationHashMap.containsKey(hostid);
        }
        return super.exist();
    }

    @Override
    public void select() throws WaarpDatabaseException {
        if (dbSession == null) {
            DbHostConfiguration conf = dbR66HostConfigurationHashMap.get(this.hostid);
            if (conf == null) {
                throw new WaarpDatabaseNoDataException("No row found");
            } else {
                // copy info
                for (int i = 0; i < allFields.length; i++) {
                    allFields[i].setValue(conf.allFields[i].getValue());
                }
                setFromArray();
                isSaved = true;
                return;
            }
        }
        super.select();
    }

    @Override
    public void update() throws WaarpDatabaseException {
        if (isSaved) {
            return;
        }
        if (dbSession == null) {
            dbR66HostConfigurationHashMap.put(this.hostid, this);
            isSaved = true;
            if (this.updatedInfo == UpdatedInfo.TOSUBMIT.ordinal()) {
                CommanderNoDb.todoList.add(this);
            }
            return;
        }
        super.update();
    }

    /**
     * Private constructor for Commander only
     */
    private DbHostConfiguration(DbSession session) {
        super(session);
    }

    /**
     * For instance from Commander when getting updated information
     * 
     * @param preparedStatement
     * @return the next updated Configuration
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbHostConfiguration getFromStatement(DbPreparedStatement preparedStatement)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbHostConfiguration dbConfiguration = new DbHostConfiguration(preparedStatement.getDbSession());
        dbConfiguration.getValues(preparedStatement, dbConfiguration.allFields);
        dbConfiguration.setFromArray();
        dbConfiguration.isSaved = true;
        return dbConfiguration;
    }

    /**
     * 
     * @return the DbPreparedStatement for getting Updated Object
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getUpdatedPrepareStament(DbSession session)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        String request = "SELECT " + selectAllFields;
        request += " FROM " + table +
                " WHERE " + Columns.UPDATEDINFO.name() + " = " +
                AbstractDbData.UpdatedInfo.TOSUBMIT.ordinal() +
                " AND " + Columns.HOSTID.name() + " = '" + Configuration.configuration.getHOST_ID()
                + "'";
        DbPreparedStatement prep = new DbPreparedStatement(session, request);
        return prep;
    }

    /**
     * 
     * @param session
     * @param hostid
     * @param business
     * @param role
     * @param alias
     * @param other
     * @return a preparedStatement with the filter set
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    public static DbPreparedStatement getFilterPrepareStament(DbSession session,
            String hostid, String business, String role, String alias, String other)
            throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        DbPreparedStatement preparedStatement = new DbPreparedStatement(session);
        String request = "SELECT " + selectAllFields + " FROM " + table;
        String condition = null;
        if (hostid != null) {
            condition = " WHERE " + Columns.HOSTID.name() + " LIKE '%" + hostid + "%' ";
        }
        if (business != null) {
            if (condition != null) {
                condition += " AND " + Columns.BUSINESS.name() + " LIKE '%" + business + "%' ";
            } else {
                condition = " WHERE " + Columns.BUSINESS.name() + " LIKE '%" + business + "%' ";
            }
        }
        if (role != null) {
            if (condition != null) {
                condition += " AND " + Columns.ROLES.name() + " LIKE '%" + role + "%' ";
            } else {
                condition = " WHERE " + Columns.ROLES.name() + " LIKE '%" + role + "%' ";
            }
        }
        if (alias != null) {
            if (condition != null) {
                condition += " AND " + Columns.ALIASES.name() + " LIKE '%" + alias + "%' ";
            } else {
                condition = " WHERE " + Columns.ALIASES.name() + " LIKE '%" + alias + "%' ";
            }
        }
        if (other != null) {
            if (condition != null) {
                condition += " AND " + Columns.OTHERS.name() + " LIKE '%" + other + "%' ";
            } else {
                condition = " WHERE " + Columns.OTHERS.name() + " LIKE '%" + other + "%' ";
            }
        }
        if (condition != null) {
            preparedStatement.createPrepareStatement(request + condition +
                    " ORDER BY " + Columns.HOSTID.name());
        } else {
            preparedStatement.createPrepareStatement(request +
                    " ORDER BY " + Columns.HOSTID.name());
        }
        return preparedStatement;
    }

    @Override
    public void changeUpdatedInfo(UpdatedInfo info) {
        if (updatedInfo != info.ordinal()) {
            updatedInfo = info.ordinal();
            allFields[Columns.UPDATEDINFO.ordinal()].setValue(updatedInfo);
            isSaved = false;
        }
    }

    /**
     * Update configuration according to new values
     */
    public void updateConfiguration() {
        updateHostConfiguration(Configuration.configuration, this);
    }

    /**
     * 
     * @return True if this Configuration refers to the current host
     */
    public boolean isOwnConfiguration() {
        return this.hostid.equals(Configuration.configuration.getHOST_ID());
    }

    /**
     * Shortcut to add all paths element from source into set
     * 
     * @param source
     * @param path
     * @param set
     * @return True if ok
     */
    private boolean updateSet(String source, String path, HashSet<String> set) {
        if (source != null && !source.isEmpty()) {
            Document document = null;
            StringReader reader = null;
            if (source != null && !source.isEmpty()) {
                try {
                    reader = new StringReader(source);
                    document = new SAXReader().read(reader);
                } catch (DocumentException e) {
                    logger.error("Unable to read the XML Config " + path + " string: " + source, e);
                    if (reader != null) {
                        reader.close();
                    }
                    return false;
                }
                if (document == null) {
                    logger.error("Unable to read the XML Config " + path + " string: " + source);
                    if (reader != null) {
                        reader.close();
                    }
                    return false;
                }
                @SuppressWarnings("unchecked")
                List<Element> list = document.selectNodes(path);
                for (Element element : list) {
                    String sval = element.getText().trim();
                    if (sval.isEmpty()) {
                        continue;
                    }
                    set.add(sval.trim());
                }
                list.clear();
                document.clearContent();
                document = null;
                if (reader != null) {
                    reader.close();
                    reader = null;
                }
            }
        }
        return true;
    }

    /**
     * update Business with possible purge and new or added content, and updating in memory information
     * 
     * @param config
     * @param newbusiness
     * @param purged
     * @return True if updated
     */
    public boolean updateBusiness(Configuration config, String newbusiness, boolean purged) {
        HashSet<String> set = new HashSet<String>();
        if (!updateSet(newbusiness, XML_BUSINESS + "/" + XML_BUSINESSID, set)) {
            return false;
        }
        if (purged) {
            config.getBusinessWhiteSet().clear();
        } else {
            String business = getBusiness();
            if (!updateSet(business, XML_BUSINESS + "/" + XML_BUSINESSID, set)) {
                return false;
            }
        }
        config.getBusinessWhiteSet().addAll(set);
        if ((newbusiness != null && !newbusiness.isEmpty()) || purged) {
            Document document = DocumentHelper.createDocument(DocumentHelper.createElement(XML_BUSINESS));
            Element root = document.getRootElement();
            for (String sval : set) {
                root.addElement(XML_BUSINESSID).setText(sval);
                logger.info("Business Allow: " + sval);
            }
            setBusiness(root.asXML());
            try {
                update();
            } catch (WaarpDatabaseException e) {
                document.clearContent();
                document = null;
                return false;
            }
            document.clearContent();
            document = null;
        }
        set.clear();
        set = null;
        return true;
    }

    /**
     * Shortcut to add all paths element with key and value from source into map
     * 
     * @param source
     * @param path
     * @param keypath
     * @param valpath
     * @param split
     * @param map
     * @return True if ok
     */
    private boolean updateMap(String source, String path, String keypath, String valpath, String split,
            HashMap<String, HashSet<String>> map) {
        if (source != null && !source.isEmpty()) {
            Document document = null;
            StringReader reader = null;
            if (source != null && !source.isEmpty()) {
                try {
                    reader = new StringReader(source);
                    document = new SAXReader().read(reader);
                } catch (DocumentException e) {
                    logger.error("Unable to read the XML Config " + path + " string: " + source, e);
                    if (reader != null) {
                        reader.close();
                    }
                    return false;
                }
                if (document == null) {
                    logger.error("Unable to read the XML Config " + path + " string: " + source);
                    if (reader != null) {
                        reader.close();
                    }
                    return false;
                }
                @SuppressWarnings("unchecked")
                List<Element> list = document.selectNodes(path);
                for (Element element : list) {
                    Element nodeid = (Element) element.selectSingleNode(keypath);
                    if (nodeid == null) {
                        continue;
                    }
                    Element nodeset = (Element) element.selectSingleNode(valpath);
                    if (nodeset == null) {
                        continue;
                    }
                    String refHostId = nodeid.getText();
                    String aliasesid = nodeset.getText();
                    String[] aliasid = aliasesid.split(split);
                    HashSet<String> set = null;
                    if (map.containsKey(refHostId)) {
                        set = map.get(refHostId);
                    } else {
                        set = new HashSet<String>();
                    }
                    for (String namealias : aliasid) {
                        set.add(namealias);
                    }
                    map.put(refHostId, set);
                }
                list.clear();
                document.clearContent();
                document = null;
                if (reader != null) {
                    reader.close();
                    reader = null;
                }
            }
        }
        return true;
    }

    /**
     * update Alias with possible purge and new or added content, and updating in memory information
     * 
     * @param config
     * @param newalias
     * @param purged
     * @return True if updated
     */
    public boolean updateAlias(Configuration config, String newalias, boolean purged) {
        HashMap<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();
        if (!updateMap(newalias, XML_ALIASES + "/" + XML_ALIAS, XML_REALID, XML_ALIASID, " |\\|", map)) {
            return false;
        }
        if (purged) {
            config.getReverseAliases().clear();
            config.getAliases().clear();
        } else {
            String alias = getAliases();
            if (!updateMap(alias, XML_ALIASES + "/" + XML_ALIAS, XML_REALID, XML_ALIASID, " |\\|", map)) {
                return false;
            }
        }
        if ((newalias != null && !newalias.isEmpty()) || purged) {
            Document document = DocumentHelper.createDocument(DocumentHelper.createElement(XML_ALIASES));
            Element root = document.getRootElement();
            for (Entry<String, HashSet<String>> entry : map.entrySet()) {
                Element elt = root.addElement(XML_ALIAS);
                elt.addElement(XML_REALID).setText(entry.getKey());
                String cumul = null;
                String[] oldAlias = config.getReverseAliases().get(entry.getKey());
                int size = oldAlias == null ? 0 : oldAlias.length;
                String[] alias = new String[entry.getValue().size() + size];
                int i = 0;
                for (; i < size; i++) {
                    alias[i] = oldAlias[i];
                }
                for (String namealias : entry.getValue()) {
                    config.getAliases().put(namealias, entry.getKey());
                    if (cumul == null) {
                        cumul = namealias;
                    } else {
                        cumul += " " + namealias;
                    }
                    alias[i] = namealias;
                    i++;
                }
                elt.addElement(XML_ALIASID).setText(cumul);
                config.getReverseAliases().put(entry.getKey(), alias);
            }
            setAliases(root.asXML());
            try {
                update();
            } catch (WaarpDatabaseException e) {
                document.clearContent();
                document = null;
                return false;
            }
            document.clearContent();
            document = null;
        } else {
            for (Entry<String, HashSet<String>> entry : map.entrySet()) {
                String[] oldAlias = config.getReverseAliases().get(entry.getKey());
                int size = oldAlias == null ? 0 : oldAlias.length;
                String[] alias = new String[entry.getValue().size() + size];
                int i = 0;
                for (; i < size; i++) {
                    alias[i] = oldAlias[i];
                }
                for (String namealias : entry.getValue()) {
                    config.getAliases().put(namealias, entry.getKey());
                    alias[i] = namealias;
                    i++;
                }
                config.getReverseAliases().put(entry.getKey(), alias);
            }
        }
        map.clear();
        map = null;
        return true;
    }

    /**
     * update Roles with possible purge and new or added content, and updating in memory information
     * 
     * @param config
     * @param newroles
     * @param purged
     * @return True if ok
     */
    public boolean updateRoles(Configuration config, String newroles, boolean purged) {
        HashMap<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();
        if (!updateMap(newroles, XML_ROLES + "/" + XML_ROLE, XML_ROLEID, XML_ROLESET, " |\\|", map)) {
            return false;
        }
        if (purged) {
            config.getRoles().clear();
        } else {
            String roles = getRoles();
            if (!updateMap(roles, XML_ROLES + "/" + XML_ROLE, XML_ROLEID, XML_ROLESET, " |\\|", map)) {
                return false;
            }
        }
        if ((newroles != null && !newroles.isEmpty()) || purged) {
            Document document = DocumentHelper.createDocument(DocumentHelper.createElement(XML_ROLES));
            Element root = document.getRootElement();
            for (Entry<String, HashSet<String>> entry : map.entrySet()) {
                RoleDefault newrole = new RoleDefault();
                Element elt = root.addElement(XML_ROLE);
                elt.addElement(XML_ROLEID).setText(entry.getKey());
                String cumul = null;
                if (entry.getValue().contains(ROLE.NOACCESS.name())) {
                    newrole.setRole(ROLE.NOACCESS);
                    cumul = ROLE.NOACCESS.name();
                }
                for (String namerole : entry.getValue()) {
                    try {
                        RoleDefault.ROLE roletype = RoleDefault.ROLE.valueOf(namerole.toUpperCase());
                        if (roletype != ROLE.NOACCESS) {
                            newrole.addRole(roletype);
                            if (cumul == null) {
                                cumul = namerole.toUpperCase();
                            } else {
                                cumul += " " + namerole.toUpperCase();
                            }
                        }
                    } catch (IllegalArgumentException e) {
                        // ignore
                    }
                }
                logger.info("New Role: " + entry.getKey() + ":" + newrole);
                config.getRoles().put(entry.getKey(), newrole);
                elt.addElement(XML_ROLESET).setText(cumul);
            }
            setRoles(root.asXML());
            try {
                update();
            } catch (WaarpDatabaseException e) {
                document.clearContent();
                document = null;
                return false;
            }
            document.clearContent();
            document = null;
        } else {
            for (Entry<String, HashSet<String>> entry : map.entrySet()) {
                RoleDefault newrole = new RoleDefault();
                if (entry.getValue().contains(ROLE.NOACCESS.name())) {
                    newrole.setRole(ROLE.NOACCESS);
                }
                for (String namerole : entry.getValue()) {
                    try {
                        RoleDefault.ROLE roletype = RoleDefault.ROLE.valueOf(namerole.toUpperCase());
                        if (roletype != ROLE.NOACCESS) {
                            newrole.addRole(roletype);
                        }
                    } catch (IllegalArgumentException e) {
                        // ignore
                    }
                }
                logger.info("New Role: " + entry.getKey() + ":" + newrole);
                config.getRoles().put(entry.getKey(), newrole);
            }
        }
        map.clear();
        map = null;
        return true;
    }

    public static void updateHostConfiguration(Configuration config, DbHostConfiguration hostConfiguration) {
        hostConfiguration.updateBusiness(config, null, false);
        hostConfiguration.updateAlias(config, null, false);
        hostConfiguration.updateRoles(config, null, false);
    }

    /**
     * 
     * @param dbSession
     * @param hostid
     * @return the version of the database from HostConfiguration table
     */
    public static String getVersionDb(DbSession dbSession, String hostid) {
        DbHostConfiguration hostConfiguration;
        try {
            hostConfiguration = new DbHostConfiguration(dbSession, hostid);
        } catch (WaarpDatabaseException e) {
            // ignore and return
            return "1.1.0";
        }
        Element others = hostConfiguration.getOtherElement();
        if (others != null) {
            Element version = (Element) others.selectSingleNode(DbHostConfiguration.OtherFields.version.name());
            if (version != null) {
                return version.getText();
            }
        }
        return "1.1.0";
    }

    public boolean isSeeAllId(String id) {
        Element others = this.getOtherElement();
        if (others != null) {
            Element seeallids = (Element) others.selectSingleNode(DbHostConfiguration.OtherFields.seeallid.name());
            if (seeallids != null) {
                String[] split = seeallids.getText().split(",");
                for (String string : split) {
                    if (string.equals(id)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Update the version for this HostId
     * 
     * @param dbSession
     * @param hostid
     * @param version
     */
    public static void updateVersionDb(DbSession dbSession, String hostid, String version) {
        DbHostConfiguration hostConfiguration;
        try {
            hostConfiguration = new DbHostConfiguration(dbSession, hostid);
        } catch (WaarpDatabaseNoDataException e) {
            hostConfiguration = new DbHostConfiguration(dbSession, hostid, "", "", "", "");
            try {
                hostConfiguration.insert();
            } catch (WaarpDatabaseException e1) {
                logger.debug("Not inserted?", e1);
                // ignore and return
                return;
            }
        } catch (WaarpDatabaseException e) {
            logger.debug("Not found?", e);
            // ignore and return
            return;
        }
        Element others = hostConfiguration.getOtherElement();
        if (others != null) {
            Element eversion = (Element) others.selectSingleNode(DbHostConfiguration.OtherFields.version.name());
            if (eversion != null) {
                eversion.setText(version);
            } else {
                others.addElement(DbHostConfiguration.OtherFields.version.name()).addText(Version.ID);
            }
        } else {
            others = DocumentHelper.createElement(DbHostConfiguration.OtherFields.root.name());
            others.addElement(DbHostConfiguration.OtherFields.version.name()).addText(Version.ID);
        }
        hostConfiguration.setOtherElement(others);
        try {
            hostConfiguration.update();
        } catch (WaarpDatabaseException e) {
            logger.debug("Not update?", e);
            // ignore
            return;
        }
    }

    /**
     * 
     * @return the DbValue associated with this table
     */
    public static DbValue[] getAllType() {
        DbHostConfiguration item = new DbHostConfiguration(null);
        return item.allFields;
    }
}
