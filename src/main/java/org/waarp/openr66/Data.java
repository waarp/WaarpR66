package org.waarp.openr66;

import org.waarp.common.database.ConnectionFactory;
import org.waarp.common.database.ScriptRunner;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.database.properties.R66DbProperties;
import org.waarp.openr66.pojo.Rule;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;

// TODO add logs
public class Data {

    public static boolean init = false;
    private static String rulePath = "";

    public static void getParams(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-initdb")) {
                init = true;
            } else if (args[i].equals("-loadrules")) {
                i++;
                rulePath = args[i];
            }
        }
    }

    public static int main(String[] args) {
        getParams(args);
        if(init && !initDB()) {
            return 1;
        }
        if (!rulePath.equals("") && loadRules(rulePath)) {
            return 1;
        }
        return 0;
    }

    private static boolean initDB() {
        ConnectionFactory factory = ConnectionFactory.getInstance();
        R66DbProperties prop = R66DbProperties.getInstance(
                factory.getProperties());
        String script = prop.getCreateQuery();
        Connection con = null;
        try {
            con = factory.getConnection();
            ScriptRunner runner = new ScriptRunner(con, false, true);
            runner.runScript(new StringReader(script));
            con.commit();
        } catch (SQLException e) {
            //logger.error("Error while initializing database", e);
            return false;
        } catch (IOException e) {
            //logger.error("Error while initializing database", e);
            return false;
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    //logger.warn(
                    //        "Cannot properly close database connection");
                }
            }
        }
        return true;
    }

    private static boolean loadRules(String path) {
        Rule[] rules = null;
        // TODO Get Rules from provided files

        RuleDAO ruleAccess = null;
        try {
            ruleAccess = DAOFactory.getInstance().getRuleDAO();
            for (int i = 0; i < rules.length; i++) {
                Rule rule = rules[i];
                if (ruleAccess.exist(rule.getName())) {
                    ruleAccess.update(rule);
                } else {
                    ruleAccess.insert(rule);
                }
            }
        } catch (DAOException e) {
            //logger.error("Error while loading rules", e);
            return false;
        } finally {
            if (ruleAccess != null) {
                ruleAccess.close();
            }
        }
        return true;
    }
}
