package server.configuration;

import com.google.gson.annotations.Expose;
import models.Host;
import utils.Strings;

import java.util.List;

/**
 * Responsible for holding static configuration details needed initially by the server to run.
 *
 * @author Palak Jain
 */
public class Config {
    @Expose
    private List<Host> members;
    @Expose
    private String dbUrl;
    @Expose
    private String username;
    @Expose
    private String password;

    /**
     * Get the details of other members in the system
     */
    public List<Host> getMembers() {
        return members;
    }

    /**
     * Get the DB connection URL
     */
    public String getDbUrl() {
        return dbUrl;
    }

    /**
     * Get the username required to log in to the system
     */
    public String getUsername() {
        return username;
    }

    /**
     * Get the password required to log in to the system
     */
    public String getPassword() {
        return password;
    }

    /**
     * Checks whether the config is valid or not
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(dbUrl) && !Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password) && members != null && members.size() > 0;
    }
}
