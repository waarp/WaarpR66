package org.waarp.openr66.configuration;

import org.waarp.common.xml.XmlHash;

public class Configuration {
    public static DatabaseConfiguration database;
    
    public static void readFromXML(XmlHash xml) {
        database.readFromXML(new XmlHash(xml.get("database").getSubXml()));
    } 

    public static void close() {
        database.close();
    }
}
