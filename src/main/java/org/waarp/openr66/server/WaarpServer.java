package org.waarp.openr66.server;

import java.lang.Exception;

import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.configuration.Configuration;
import org.waarp.openr66.configuration.BadConfigurationException;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;


public class WaarpServer {

    private static WaarpLogger logger;
    public static String configurationFile = null;

    public static void getParams(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i] == "--conf" || args[i] == "-c") {
                i++;
                configurationFile = args[i];
            } 
        }
        if (configurationFile == null) {
            throw new Exception("Configuration not found (--conf | -c)");
        }
    }

    public static void readConfiguration(String filePath) 
            throws BadConfigurationException {
            Configuration.init(filePath);
            Configuration.database.init(Configuration.get());
            // Load other modules
    }

    public static void main(String[] args) {
        // Init LoggerFactory
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
        logger = WaarpLoggerFactory.getLogger(WaarpServer.class);
        // Get Parameters
        try {
            getParams(args);
        } catch (Exception e) {
            System.err.println("Unable to parse command line: " 
                    + e.getMessage());
            System.out.println("man");
            System.exit(1);
        }
        // Read Configuration
        try {
            readConfiguration(configurationFile);
        } catch (BadConfigurationException e) {
            logger.error(e);
            System.err.println("Bad configuration check logs for more details");
            System.exit(1);
        }
        // Register to Shutdown Hook to allow server restart
        R66ShutdownHook.registerMain(R66Server.class, args);

        //Start server
        
        //Exit cleanly
        System.exit(0);
    }
}
