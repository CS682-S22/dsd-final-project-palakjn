package application.configuration;

import com.google.gson.annotations.Expose;
import models.Host;
import utils.Strings;

/**
 * Responsible for holding values responsible to start the client.
 *
 * @author Palak Jain
 */
public class Config {
    @Expose
    private Host local;
    @Expose
    private Host leader;
    @Expose
    private String location;

    /**
     * Get the details of local client
     */
    public Host getLocal() {
        return local;
    }

    /**
     * Get the details of leader
     */
    public Host getLeader() {
        return leader;
    }

    /**
     * Set the new leader
     */
    public void setLeader(Host leader) {
        this.leader = leader;
    }

    /**
     * Get the location of the log to send
     */
    public String getLocation() {
        return location;
    }

    /**
     * Checks whether the values given by client is valid or not
     */
    public boolean isValid() {
        return  local != null && local.isValid() &&
                leader != null && leader.isValid() &&
                !Strings.isNullOrEmpty(location);
    }
}
