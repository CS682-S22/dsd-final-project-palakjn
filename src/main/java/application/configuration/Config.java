package application.configuration;

import com.google.gson.annotations.Expose;
import models.Host;
import utils.Strings;

import java.util.List;

/**
 * Responsible for holding values responsible to start the client.
 *
 * @author Palak Jain
 */
public class Config {
    @Expose
    private Host local;
    @Expose
    private List<Host> members;
    @Expose
    private String location;
    @Expose
    private boolean isProducer;
    @Expose
    private boolean pressEnterToSend;
    @Expose
    private boolean isConsumer;
    @Expose
    private int offset;
    @Expose
    private int numOfLogs;

    /**
     * Get the details of local client
     */
    public Host getLocal() {
        return local;
    }

    /**
     * Get the member at the given index
     */
    public Host getMember(int index) {
        return members.get(index);
    }

    /**
     * Get the number of members in the system
     */
    public int getMemberCount() {
        return members.size();
    }

    /**
     * Get the location of the log to send
     */
    public String getLocation() {
        return location;
    }

    /**
     * Gets whether the host is producer
     */
    public boolean isProducer() {
        return isProducer;
    }

    /**
     * Gets whether to press enter to send new log
     */
    public boolean isPressEnterToSend() {
        return pressEnterToSend;
    }

    /**
     * Gets whether the host is consumer
     */
    public boolean isConsumer() {
        return isConsumer;
    }

    /**
     * Get the starting offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Set the starting offset
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * Get number of logs to read in single call
     */
    public int getNumOfLogs() {
        return numOfLogs;
    }

    /**
     * Checks whether the values given by client is valid or not
     */
    public boolean isValid() {
        boolean isValid =  local != null && local.isValid() &&
                            members != null && members.size() > 0 &&
                            !Strings.isNullOrEmpty(location) &&
                            !(isConsumer && isProducer) &&
                            !(!isConsumer && !isProducer) &&
                            (!isConsumer || (offset >= 0 && numOfLogs > 0));

        if (isValid) {
            for (Host member : members) {
                isValid = member.isValid() && isValid;
            }
        }

        return isValid;
    }
}
