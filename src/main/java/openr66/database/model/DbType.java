package openr66.database.model;

/**
 * Type of Database supported
 * @author Frederic Bregier
 *
 */
public enum DbType {
    Oracle, MySQL, PostGreSQL, H2;

    public static DbType getFromDriver(String driver) {
        if (driver.contains("oracle")) {
            return DbType.Oracle;
        } else if (driver.contains("mysql")) {
            return DbType.MySQL;
        } else if (driver.contains("postgresql")) {
            return DbType.PostGreSQL;
        } else if (driver.contains("h2")) {
            return DbType.H2;
        }
        return null;
    }
}