package consensus.configuration;

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
    private Host local;
    @Expose
    private List<Host> members;
    @Expose
    private String dbUrl;
    @Expose
    private String username;
    @Expose
    private String password;
    @Expose
    private String location;

    /**
     * Get the detail of local running instance
     */
    public Host getLocal() {
        return local;
    }

    /**
     * Get the member at the given index
     */
    public Host getMember(int index) {
        Host host = null;

        if (members != null && index < members.size()) {
            host = members.get(index);
        }

        return host;
    }

    /**
     * Get the number of members
     */
    public int getNumOfMembers() {
        return members.size();
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
     * Get the file location
     */
    public String getLocation() {
        return location;
    }

    /**
     * Set the file location
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /**
     * Checks whether the config is valid or not
     */
    public boolean isValid() {
        boolean isValid = local != null &&
                          local.isValid() &&
                          !Strings.isNullOrEmpty(dbUrl) &&
                          !Strings.isNullOrEmpty(username) &&
                          !Strings.isNullOrEmpty(password) &&
                          members != null &&
                          members.size() > 0 &&
                          !Strings.isNullOrEmpty(location);
        if (members != null) {
            for (Host member : members) {
                isValid = member.isValid() && isValid;
            }
        }

        return isValid;
    }
}
