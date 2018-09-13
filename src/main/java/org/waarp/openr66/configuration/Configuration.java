package org.waarp.openr66.configuration;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class Configuration {
    private static FileBasedConfiguration conf = null;

    // Configuration classes
    public static DatabaseConfiguration database;
    
    public static void init(String filepath) throws BadConfigurationException {
        if (filepath == null || filepath == "") {
             throw new BadConfigurationException("Empty configuration");
        }
        // Load Configuration
        try {
            conf = new Configurations().xml(filepath);
        } catch (ConfigurationException e) {
             throw new BadConfigurationException("Cannot read configuration");
        }
    }

    public static FileBasedConfiguration get() {
         return conf;
    }
}
