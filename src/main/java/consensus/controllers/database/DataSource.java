package consensus.controllers.database;
import org.apache.commons.dbcp2.BasicDataSource;
import consensus.configuration.Config;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Maintains the connection pool.
 *
 * @author Palak Jain
 */
public class DataSource {
    private static BasicDataSource ds;

    /**
     * Initialize the DB connection pool
     */
    public static void init(Config config) {
        if(ds == null) {
            ds = new BasicDataSource();
            ds.setUrl(config.getDbUrl());
            ds.setUsername(config.getUsername());
            ds.setPassword(config.getPassword());
            ds.setMinIdle(5);
            ds.setMaxIdle(10);
        }
    }

    /**
     * Return a Connection from the pool.
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
