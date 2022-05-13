package models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for holding values required to pull logs from the broker
 */
public class PullRequest {
    @Expose
    private int fromOffset;
    @Expose
    private int numOfLogs;

    public PullRequest(int fromOffset, int numOfLogs) {
        this.fromOffset = fromOffset;
        this.numOfLogs = numOfLogs;
    }

    /**
     * Get the offset from where to get the data
     */
    public int getFromOffset() {
        return fromOffset;
    }

    /**
     * Get the number of logs to get within the response
     */
    public int getNumOfLogs() {
        return numOfLogs;
    }
}
